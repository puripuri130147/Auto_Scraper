# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import time
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ======================================================================
# CONFIG
# ======================================================================
HOME = os.getenv("TMD_HOME", "https://www.tmd.go.th")
CSV_OUT = os.getenv("CSV_OUT", "tmd_7day_forecast_today.csv")

PAGELOAD_TIMEOUT = 60
SCRIPT_TIMEOUT = 60
WAIT_MED = 40
WAIT_LONG = 60
RETRIES_PER_PROVINCE = 2
MAX_SCRAPE_PASSES = 5
SLEEP_MIN = 0.7
SLEEP_MAX = 1.2
PAGE_LOAD_STRATEGY = "none"

RE_INT = re.compile(r"(\d+)")

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
    opt.page_load_strategy = PAGE_LOAD_STRATEGY

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

def open_home_ready(driver):
    safe_get(driver, HOME, timeout=WAIT_MED)
    WebDriverWait(driver, WAIT_LONG).until(
        EC.presence_of_element_located((By.ID, "province-selector"))
    )

def collect_mapping_from_select(driver) -> Dict[str, str]:
    MAX_TRIES = 5
    for attempt in range(1, MAX_TRIES + 1):
        print(f"🔄 พยายามดึง select list รอบที่ {attempt}")
        try:
            sel = WebDriverWait(driver, WAIT_LONG).until(
                EC.visibility_of_element_located((By.ID, "province-selector"))
            )
            driver.execute_script("arguments[0].focus();", sel)
            driver.execute_script("arguments[0].click();", sel)
            time.sleep(0.5)

            options = sel.find_elements(By.TAG_NAME, "option")
            mapping = {
                (op.text or "").strip(): (op.get_attribute("value") or "").strip()
                for op in options
                if (op.text or "").strip() and (op.get_attribute("value") or "").strip() and not op.text.startswith("เลือก")
            }

            print(f"🔹 เจอ {len(mapping)} จังหวัด")
            if len(mapping) >= 10:
                return mapping
            else:
                print(f"⚠️ option น้อยเกินไป, refresh หน้าเว็บ")
        except TimeoutException:
            print("⚠️ ไม่เจอ select element, refresh หน้าเว็บ")
        driver.refresh()
        time.sleep(2)

    raise TimeoutException("อ่านรายชื่อจังหวัดไม่สำเร็จ")

def _js_set_select_value(driver, value: str) -> bool:
    js = """
    var s=document.getElementById('province-selector');
    if(!s)return false;
    s.value=arguments[0];
    s.dispatchEvent(new Event('change',{bubbles:true}));
    return true;
    """
    return bool(driver.execute_script(js, value))

def select_province(driver, province_name: str, mapping: Dict[str, str]) -> bool:
    val = mapping.get(province_name, "")
    if not val:
        return False
    ok = _js_set_select_value(driver, val)
    if ok:
        time.sleep(0.2)
    return ok

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
# SCRAPE LOOP
# ======================================================================
def _try_scrape_provinces(driver, names: List[str], retries_per_province: int, mapping: Dict[str, str]) -> Tuple[List[Dict[str,str]], List[str]]:
    rows: List[Dict[str,str]] = []
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
                driver.refresh()
                time.sleep(1)
            except Exception as e:
                if attempt < retries_per_province - 1:
                    driver.refresh()
                    time.sleep(1)
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
    all_rows: List[Dict[str,str]] = []
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
        driver.quit()

    new_df = pd.DataFrame(all_rows)
    if not new_df.empty:
        new_df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
        print(f"\n📝 บันทึกข้อมูลลงไฟล์: {CSV_OUT}")
    else:
        print("⚠️ ไม่ได้ข้อมูลใหม่")

# ======================================================================
# ENTRY
# ======================================================================
if __name__ == "__main__":
    main()
