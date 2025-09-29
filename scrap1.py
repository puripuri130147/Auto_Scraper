# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ======================================================================
# CONFIG
# ======================================================================
HOME: str = os.getenv("TMD_HOME", "https://www.tmd.go.th")
CSV_OUT: str = os.getenv("CSV_OUT", "tmd_7day_forecast_today.csv")

PAGELOAD_TIMEOUT = 60
SCRIPT_TIMEOUT = 60
WAIT_MED = 40
WAIT_LONG = 60

RETRIES_PER_PROVINCE = 2
MAX_SCRAPE_PASSES = 5

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

def open_home_ready(driver) -> None:
    safe_get(driver, HOME, timeout=WAIT_MED)
    WebDriverWait(driver, WAIT_LONG).until(
        EC.presence_of_element_located((By.ID, "province-selector"))
    )

def collect_mapping_from_select(driver) -> Dict[str, str]:
    MAX_TRIES = 5
    for attempt in range(1, MAX_TRIES + 1):
        try:
            sel = WebDriverWait(driver, WAIT_LONG).until(
                EC.visibility_of_element_located((By.ID, "province-selector"))
            )
            options = sel.find_elements(By.TAG_NAME, "option")
            mapping: Dict[str, str] = {}
            for op in options:
                name = (op.text or "").strip()
                val = (op.get_attribute("value") or "").strip()
                if not name or not val or name.startswith("เลือก"):
                    continue
                mapping[name] = val
            if len(mapping) >= 10:
                return mapping
        except Exception:
            pass
        driver.refresh()
        time.sleep(1)
    raise TimeoutException("อ่านรายชื่อจังหวัดได้น้อยผิดปกติ")

def select_province(driver, province_name: str, mapping: Dict[str, str]) -> bool:
    val = mapping.get(province_name, "")
    if not val:
        return False
    try:
        sel_elem = driver.find_element(By.ID, "province-selector")
        Select(sel_elem).select_by_value(val)
        time.sleep(0.5)  # รอให้หน้าเว็บโหลดข้อมูล
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
                rain_chance = int(''.join(filter(str.isdigit, rain_text))) / 100
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
            except (StaleElementReferenceException, TimeoutException):
                driver.refresh()
                time.sleep(0.8)
            except Exception as e:
                if attempt < retries_per_province - 1:
                    driver.refresh()
                    time.sleep(0.8)
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
