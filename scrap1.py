# -*- coding: utf-8 -*-
from __future__ import annotations

import os, time, random
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ======================================================================
# CONFIG
# ======================================================================
HOME: str = os.getenv("TMD_HOME", "https://www.tmd.go.th")
CSV_OUT: str = os.getenv("CSV_OUT", "tmd_7day_forecast_today.csv")

PAGELOAD_TIMEOUT = 60
SCRIPT_TIMEOUT   = 60
WAIT_MED         = 45
WAIT_LONG        = 75

RETRIES_PER_PROVINCE = 2
MAX_SCRAPE_PASSES    = 5

SLEEP_MIN = 0.7
SLEEP_MAX = 1.2

# ======================================================================
# SELENIUM HELPERS
# ======================================================================
def make_driver() -> webdriver.Chrome:
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--disable-gpu")
    opt.add_argument("--window-size=1920,1080")
    # ทำให้ DOM คงที่ขึ้น
    opt.add_argument("--lang=th-TH")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    drv = webdriver.Chrome(service=service, options=opt)
    drv.set_page_load_timeout(PAGELOAD_TIMEOUT)
    drv.set_script_timeout(SCRIPT_TIMEOUT)
    return drv

def safe_get(driver, url, timeout=PAGELOAD_TIMEOUT):
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass

@contextmanager
def _default_content(driver):
    try:
        driver.switch_to.default_content()
        yield
    finally:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

def _try_find_select_current_context(driver) -> Optional[webdriver.remote.webelement.WebElement]:
    CANDIDATES = [
        (By.ID, "province-selector"),
        (By.CSS_SELECTOR, "select#province-selector"),
        (By.CSS_SELECTOR, "select[name*='province']"),
        (By.XPATH, "//select[contains(@id,'province') or contains(@name,'province')]"),
    ]
    for how, what in CANDIDATES:
        try:
            el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((how, what)))
            return el
        except Exception:
            pass
    return None

def find_province_select(driver) -> Optional[webdriver.remote.webelement.WebElement]:
    # 1) หน้า default
    with _default_content(driver):
        el = _try_find_select_current_context(driver)
        if el: return el
    # 2) ไล่ทุก iframe
    with _default_content(driver):
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for f in iframes:
            try:
                driver.switch_to.frame(f)
                el = _try_find_select_current_context(driver)
                if el:
                    return el
            except Exception:
                pass
            finally:
                driver.switch_to.default_content()
    return None

def scroll_into_view(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
    except Exception:
        pass

def open_home_ready(driver) -> None:
    safe_get(driver, HOME, timeout=WAIT_MED)
    if not find_province_select(driver):
        # รีเฟรชสั้น ๆ เผื่อหน้าเพิ่งสลับ layout
        driver.refresh()
        time.sleep(1.0)
        if not find_province_select(driver):
            raise TimeoutException("ไม่พบตัวเลือกจังหวัดบนหน้าแรก")

def collect_mapping_from_select(driver) -> Dict[str, str]:
    MAX_TRIES = 5
    for _ in range(MAX_TRIES):
        try:
            sel = find_province_select(driver)
            if not sel:
                raise TimeoutException("หา select จังหวัดไม่เจอ")
            options = sel.find_elements(By.TAG_NAME, "option")
            mapping: Dict[str, str] = {}
            for op in options:
                name = (op.text or "").strip()
                val  = (op.get_attribute("value") or "").strip()
                if not name or not val or name.startswith("เลือก"):
                    continue
                mapping[name] = val
            if len(mapping) >= 10:
                return mapping
        except Exception:
            pass
        driver.refresh()
        time.sleep(1.0)
    raise TimeoutException("อ่านรายชื่อจังหวัดได้น้อยผิดปกติ")

def select_province(driver, province_name: str, mapping: Dict[str, str]) -> bool:
    val = mapping.get(province_name, "")
    if not val:
        return False
    try:
        sel_elem = find_province_select(driver)
        if not sel_elem:
            raise RuntimeError("ไม่พบ select จังหวัด")

        scroll_into_view(driver, sel_elem)
        # ป้องกันกรณีคลิกไม่ได้
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, ".")))
        except Exception:
            pass

        # 1) วิธีปกติ
        try:
            Select(sel_elem).select_by_value(val)
        except Exception:
            # 2) สำรองด้วย JS + dispatch change
            driver.execute_script("""
                const s = arguments[0], v = arguments[1];
                s.value = v;
                s.dispatchEvent(new Event('input', {bubbles:true}));
                s.dispatchEvent(new Event('change', {bubbles:true}));
            """, sel_elem, val)

        time.sleep(0.7)
        return True
    except Exception as e:
        print(f"[{province_name}] ⚠️ select error: {e}")
        return False

def wait_rain_info(driver):
    WebDriverWait(driver, WAIT_LONG).until(
        lambda d: len(d.find_elements(By.XPATH, "//div[contains(text(),'%')]")) > 0
    )

def parse_today_fast(driver, province_name: str) -> Optional[Dict[str, str]]:
    cards = driver.find_elements(By.CSS_SELECTOR, "div.card.card-shadow.text-center")
    for c in cards:
        try:
            head = c.find_element(By.CSS_SELECTOR, "div.font-small")
            if head.text.strip() != "วันนี้":
                continue
            tiny = c.find_elements(By.CSS_SELECTOR, "div.font-tiny.text-center")
            cond, rain_text = None, None
            for el in tiny:
                txt = (el.text or "").strip()
                if "%" in txt and not rain_text:
                    rain_text = txt
                elif "%" not in txt and not cond:
                    cond = txt
            if cond and rain_text:
                digits = "".join(ch for ch in rain_text if ch.isdigit()) or "0"
                rain_chance = int(digits) / 100.0
                return {
                    "Province": province_name,
                    "Weather": cond,
                    "RainChance": rain_chance,
                    "DateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
        except Exception:
            continue
    return None

# ======================================================================
# INTERNAL: scrape loop
# ======================================================================
def _try_scrape_provinces(driver, names: List[str], retries_per_province: int, mapping: Dict[str, str]) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    failed: List[str] = []
    total = len(names)
    for i, name in enumerate(names, 1):
        ok = False
        for attempt in range(retries_per_province):
            try:
                if not select_province(driver, name, mapping):
                    raise RuntimeError("ตั้งค่า select ไม่สำเร็จ")

                wait_rain_info(driver)

                row = parse_today_fast(driver, name)
                if row:
                    rows.append(row)
                    ok = True
                    print(f"[{i}/{total}] {name} ✔")
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                    break
                else:
                    raise RuntimeError("อ่าน card วันนี้ ไม่สำเร็จ")
            except (StaleElementReferenceException, TimeoutException, WebDriverException):
                driver.refresh()
                time.sleep(0.9)
            except Exception as e:
                if attempt < retries_per_province - 1:
                    driver.refresh()
                    time.sleep(0.9)
                else:
                    print(f"[{i}/{total}] {name} ✖ {e}")
        if not ok:
            failed.append(name)
    return rows, failed

# ======================================================================
# MAIN
# ======================================================================
def main():
    driver = make_driver()
    all_rows: List[Dict[str, str]] = []
    failed: List[str] = []
    try:
        open_home_ready(driver)
        mapping = collect_mapping_from_select(driver)
        names = list(mapping.keys())
        print(f"พบจังหวัด {len(names)} รายการ")

        to_try = names[:]
        pass_num = 0
        prev_failed_count: Optional[int] = None

        while to_try and pass_num < MAX_SCRAPE_PASSES:
            pass_num += 1
            print(f"\nเริ่มรอบที่ {pass_num} (ลอง {len(to_try)} จังหวัด)")
            rows, failed_this = _try_scrape_provinces(driver, to_try, RETRIES_PER_PROVINCE, mapping)

            all_rows.extend(rows)
            print(f"รอบ {pass_num} สำเร็จ {len(rows)} จังหวัด, พลาด {len(failed_this)} จังหวัด")

            if not failed_this:
                print("✅ เก็บข้อมูลครบทุกจังหวัดแล้ว")
                failed = []
                break

            if prev_failed_count is not None and len(failed_this) >= prev_failed_count:
                print("⚠️ ไม่มีความคืบหน้าจากรอบก่อนหน้า")
                failed = failed_this
                break

            to_try = failed_this
            prev_failed_count = len(failed_this)
        else:
            failed = to_try if to_try else []
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    new_df = pd.DataFrame(all_rows)
    if not new_df.empty:
        new_df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
        print(f"\n📝 บันทึกข้อมูลลงไฟล์: {CSV_OUT} (rows={len(new_df)})")
    else:
        print("⚠️ ไม่ได้ข้อมูลใหม่")

# ======================================================================
if __name__ == "__main__":
    main()
