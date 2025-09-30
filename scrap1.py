# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, time, random, pathlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

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
# DEBUG UTILS
# ======================================================================
def _ensure_outdir(p="debug_artifacts"):
    d = pathlib.Path(p)
    d.mkdir(parents=True, exist_ok=True)
    return d

def dump_dom_debug(driver, tag="debug"):
    out = _ensure_outdir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = out / f"{ts}_{tag}.html"
    png_path  = out / f"{ts}_{tag}.png"
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.save_screenshot(str(png_path))
        print(f"💾 Saved debug: {html_path.name}, {png_path.name}")
    except Exception as e:
        print("⚠️ dump_dom_debug failed:", e)

def list_iframes_recursive(driver, max_depth=4, _depth=0, _acc=None):
    if _acc is None: _acc = []
    if _depth > max_depth: return _acc
    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
    except Exception:
        frames = []
    for idx, fr in enumerate(frames, 1):
        info = {
            "depth": _depth,
            "index": idx,
            "name": fr.get_attribute("name"),
            "id": fr.get_attribute("id"),
            "src": fr.get_attribute("src"),
        }
        _acc.append(info)
        try:
            driver.switch_to.frame(fr)
            list_iframes_recursive(driver, max_depth, _depth+1, _acc)
        except Exception:
            pass
        finally:
            driver.switch_to.default_content()
    return _acc

def log_iframes(driver, tag="iframes"):
    items = list_iframes_recursive(driver, max_depth=4)
    print(f"🔎 Found {len(items)} iframe(s) total:")
    for it in items:
        pad = "  " * it["depth"]
        print(f"{pad}- depth={it['depth']} idx={it['index']} id={it['id']} name={it['name']} src={it['src']}")

# ======================================================================
# ROBUST HELPERS: หา <select> ได้แม้ย้ายไปอยู่ใน iframe/combobox
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

def wait_ui_idle():
    time.sleep(0.6)

def _click_if_present(driver, by, sel):
    try:
        el = driver.find_element(by, sel)
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        return False

def try_dismiss_banners(driver):
    # เผื่อข้อความปุ่มคุกกี้/ยอมรับที่พบบ่อย
    texts = ["ยอมรับ", "ตกลง", "รับทราบ", "ปิด", "Accept", "I agree", "Got it"]
    for t in texts:
        x = f"//*[self::button or self::a or @role='button'][contains(., '{t}')]"
        try:
            btns = driver.find_elements(By.XPATH, x)
            for b in btns[:3]:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                    time.sleep(0.1)
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.2)
                except Exception:
                    pass
        except Exception:
            pass

def _try_find_select_in_context(driver):
    # 1) หา <select> จริงก่อน
    CANDS = [
        (By.ID, "province-selector"),
        (By.CSS_SELECTOR, "select#province-selector"),
        (By.CSS_SELECTOR, "select[name*='province' i]"),
        (By.XPATH, "//select[contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'province') or "
                   "contains(translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'province')]"),
    ]
    for how, what in CANDS:
        try:
            return WebDriverWait(driver, 6).until(EC.presence_of_element_located((how, what)))
        except Exception:
            pass

    # 2) เผื่อเป็น combobox (ไม่ใช่ <select>)
    CANDS_COMBO = [
        (By.XPATH, "//*[@role='combobox' and (contains(@aria-label,'จังหวัด') or contains(.,'จังหวัด'))]"),
        (By.XPATH, "//*[contains(@class,'select') and (contains(@aria-label,'จังหวัด') or contains(.,'จังหวัด'))]"),
    ]
    for how, what in CANDS_COMBO:
        try:
            el = WebDriverWait(driver, 3).until(EC.presence_of_element_located((how, what)))
            return el
        except Exception:
            pass

    return None

def find_province_select(driver):
    # search default
    with _default_content(driver):
        el = _try_find_select_in_context(driver)
        if el: return el

    # DFS ทุก iframe (ลึกสุด 4)
    def _dfs(depth=0, max_depth=4):
        if depth > max_depth:
            return None
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        for fr in frames:
            try:
                driver.switch_to.frame(fr)
                el = _try_find_select_in_context(driver)
                if el: return el
                deeper = _dfs(depth+1, max_depth)
                if deeper: return deeper
            except Exception:
                pass
            finally:
                driver.switch_to.default_content()
        return None

    with _default_content(driver):
        return _dfs()

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

    drv = webdriver.Chrome(options=opt)  # Selenium Manager จะจัดการ chromedriver
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
    wait_ui_idle()
    try_dismiss_banners(driver)

    el = find_province_select(driver)
    if not el:
        for _ in range(2):
            driver.refresh(); wait_ui_idle(); try_dismiss_banners(driver)
            el = find_province_select(driver)
            if el: break

    if not el:
        dump_dom_debug(driver, "no_select")
        log_iframes(driver)
        raise TimeoutException("ไม่พบตัวเลือกจังหวัดบนหน้าแรก (เก็บไฟล์ดีบักแล้ว)")

def collect_mapping_from_select(driver) -> Dict[str, str]:
    MAX_TRIES = 6
    for _ in range(MAX_TRIES):
        sel = find_province_select(driver)
        if sel:
            _scroll_into_view(driver, sel)
            try:
                driver.execute_script("arguments[0].focus();arguments[0].click();", sel)
                wait_ui_idle()
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

        driver.refresh(); wait_ui_idle(); try_dismiss_banners(driver)

    dump_dom_debug(driver, "map_failed")
    log_iframes(driver)
    raise TimeoutException("อ่านรายชื่อจังหวัดได้น้อยผิดปกติ (เก็บไฟล์ดีบักแล้ว)")

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
        # เรียงตาม DateTime (ใช้คอลัมน์ช่วย ไม่แก้ค่าเดิม)
        if "DateTime" in new_df.columns:
            sort_key = pd.to_datetime(new_df["DateTime"], errors="coerce")
            new_df = new_df.assign(__dt=sort_key).sort_values("__dt").drop(columns="__dt")

        new_df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
        print(f"\n📝 บันทึกข้อมูลลงไฟล์: {CSV_OUT} | rows={len(new_df)} (เรียงตาม DateTime แล้ว)")
    else:
        print("⚠️ ไม่ได้ข้อมูลใหม่")

# ======================================================================
# ENTRY
# ======================================================================
if __name__ == "__main__":
    main()
