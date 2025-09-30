# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, time, random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from contextlib import contextmanager

# ======================================================================
# CONFIG
# ======================================================================
HOME: str = os.getenv("TMD_HOME", "https://www.tmd.go.th")
CSV_OUT: str = os.getenv("CSV_OUT", "tmd_7day_forecast_today.csv")

PAGELOAD_TIMEOUT: int = int(os.getenv("PAGELOAD_TIMEOUT", "50"))
SCRIPT_TIMEOUT: int = int(os.getenv("SCRIPT_TIMEOUT", "50"))
WAIT_MED: int = int(os.getenv("WAIT_MED", "20"))
WAIT_LONG: int = int(os.getenv("WAIT_LONG", "35"))

RETRIES_PER_PROVINCE = int(os.getenv("RETRIES_PER_PROVINCE", "2"))
MAX_SCRAPE_PASSES = int(os.getenv("MAX_SCRAPE_PASSES", "5"))

SLEEP_MIN = float(os.getenv("SLEEP_MIN", "0.7"))
SLEEP_MAX = float(os.getenv("SLEEP_MAX", "1.2"))

PAGE_LOAD_STRATEGY: str = os.getenv("PAGE_LOAD_STRATEGY", "none")
RE_INT = re.compile(r"(\d+)")

# ======================================================================
# ROBUST HELPERS: หา <select> ได้แม้ย้ายไปอยู่ใน iframe
# ======================================================================
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

def _try_find_select_in_context(driver):
    CANDS = [
        (By.ID, "province-selector"),
        (By.CSS_SELECTOR, "select#province-selector"),
        (By.CSS_SELECTOR, "select[name*='province']"),
        (By.XPATH, "//select[contains(@id,'province') or contains(@name,'province')]"),
    ]
    for how, what in CANDS:
        try:
            return WebDriverWait(driver, 8).until(EC.presence_of_element_located((how, what)))
        except Exception:
            pass
    return None

def find_province_select(driver):
    # 1) default content
    with _default_content(driver):
        el = _try_find_select_in_context(driver)
        if el:
            return el
    # 2) ลองทุก iframe
    with _default_content(driver):
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for f in iframes:
            try:
                driver.switch_to.frame(f)
                el = _try_find_select_in_context(driver)
                if el:
                    return el
            except Exception:
                pass
            finally:
                driver.switch_to.default_content()
    return None

def _scroll_into_view(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
    except Exception:
        pass

# ======================================================================
# SELENIUM BOOTSTRAP
# ======================================================================
def make_driver() -> webdriver.Chrome:
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    # เพิ่มความเสถียรบน CI
    opt.add_argument("--lang=th-TH")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    opt.page_load_strategy = PAGE_LOAD_STRATEGY

    drv = webdriver.Chrome(options=opt)  # Selenium Manager จะจัดการ chromedriver ให้
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

def open_home_ready(driver) -> None:
    safe_get(driver, HOME, timeout=WAIT_MED)
    el = find_province_select(driver)
    if not el:
        for _ in range(2):
            driver.refresh(); time.sleep(1.0)
            el = find_province_select(driver)
            if el:
                break
    if not el:
        # บันทึก HTML ไว้ดีบัก
        with open("page_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        raise TimeoutException("ไม่พบตัวเลือกจังหวัดบนหน้าแรก (บันทึก page_debug.html แล้ว)")

def collect_mapping_from_select(driver) -> Dict[str, str]:
    MAX_TRIES = 6
    for _ in range(MAX_TRIES):
        sel = find_province_select(driver)
        if sel:
            _scroll_into_view(driver, sel)
            try:
                driver.execute_script("arguments[0].focus();arguments[0].click();", sel)
                time.sleep(0.2)
            except Exception:
                pass

            mapping: Dict[str, str] = {}
            try:
                options = sel.find_elements(By.TAG_NAME, "option")
                for op in options:
                    name = (op.text or "").strip()
                    val  = (op.get_attribute("value") or "").strip()
                    if not name or not val or name.startswith("เลือก"):
                        continue
                    mapping[name] = val
            except StaleElementReferenceException:
                mapping = {}

            if len(mapping) >= 10:
                return mapping

        driver.refresh(); time.sleep(1.0)

    with open("page_debug.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    raise TimeoutException("อ่านรายชื่อจังหวัดได้น้อยผิดปกติ (บันทึก page_debug.html แล้ว)")

def _js_set_select_value(driver, sel, value: str) -> bool:
    try:
        driver.execute_script("""
            const s = arguments[0], v = arguments[1];
            s.value = v;
            s.dispatchEvent(new Event('input', {bubbles:true}));
            s.dispatchEvent(new Event('change', {bubbles:true}));
        """, sel, value)
        return True
    except Exception:
        return False

def select_province(driver, province_name: str, mapping: Dict[str, str]) -> bool:
    val = mapping.get(province_name, "")
    if not val:
        return False

    sel = find_province_select(driver)
    if not sel:
        return False

    _scroll_into_view(driver, sel)

    # 1) วิธีปกติ
    try:
        from selenium.webdriver.support.ui import Select
        Select(sel).select_by_value(val)
        time.sleep(0.6)
        return True
    except Exception:
        pass

    # 2) สำรองด้วย JS
    if _js_set_select_value(driver, sel, val):
        time.sleep(0.6)
        return True

    return False

def wait_rain_info(driver):
    WebDriverWait(driver, WAIT_MED).until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'%')]"))
    )

def _extract_percent(text: str) -> Optional[float]:
    m = RE_INT.search(text or "")
    return (int(m.group(1)) / 100.0) if m else None

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
                return {
                    "Province": province_name,
                    "Weather": cond,
                    "RainChance": _extract_percent(rain_text),
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
    print(f"เริ่มดึง {total} จังหวัด")

    for i, name in enumerate(names, 1):
        ok = False
        for attempt in range(retries_per_province):
            try:
                if not select_province(driver, name, mapping):
                    raise RuntimeError("ตั้งค่า select ไม่สำเร็จ")

                WebDriverWait(driver, WAIT_MED).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.card.card-shadow.text-center"))
                )
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

            except (StaleElementReferenceException, TimeoutException):
                driver.refresh(); time.sleep(0.8)
            except Exception as e:
                if attempt < retries_per_province - 1:
                    driver.refresh(); time.sleep(0.8)
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
        print(f"\n📝 บันทึกข้อมูลลงไฟล์: {CSV_OUT} | rows={len(new_df)}")
    else:
        print("⚠️ ไม่ได้ข้อมูลใหม่")

# ======================================================================
# ENTRY
# ======================================================================
if __name__ == "__main__":
    main()
