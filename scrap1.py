# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, time, json, random
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from io import BytesIO, StringIO

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# -------- Google Drive API (Service Account) --------
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ======================================================================
# CONFIG
# ======================================================================
HOME: str = os.getenv("TMD_HOME", "https://www.tmd.go.th")
CSV_OUT: str = os.getenv("CSV_OUT", "tmd_7day_forecast_today.csv")

# เปิด/ปิดอัปโหลดขึ้น Google Drive
ENABLE_GOOGLE_DRIVE_UPLOAD: bool = os.getenv("ENABLE_GOOGLE_DRIVE_UPLOAD", "false").lower() == "true"
# ใส่เนื้อหา SA JSON ผ่าน ENV (แนะนำให้มาจาก GitHub Secret)
SERVICE_ACCOUNT_JSON: Optional[str] = os.getenv("SERVICE_ACCOUNT_JSON")
# ใช้ fileId ของไฟล์ปลายทางจาก ENV/Secret (ห้ามฮาร์ดโค้ด)
DRIVE_FILE_ID: Optional[str] = os.getenv("TMD_FILE_ID")

CSV_MIMETYPE: str = "text/csv"

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

# ================= Email Notify (ปิดเป็นค่าเริ่มต้น) =================
EMAIL_ENABLED: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER: str = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO: str = os.getenv("EMAIL_TO", "")

def send_email(subject: str, body_text: str) -> None:
    if not EMAIL_ENABLED:
        return
    try:
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        import smtplib

        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body_text, "plain", "utf-8"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, [x.strip() for x in EMAIL_TO.split(",") if x.strip()], msg.as_string())
        server.quit()
        print("📧 ส่งอีเมลแจ้งเตือนแล้ว")
    except Exception as e:
        print("⚠️ ส่งอีเมลล้มเหลว:", e)

# ======================================================================
# GOOGLE DRIVE HELPERS (Append/Merge แล้วอัปเดตไฟล์เดิม)
# ======================================================================
def _check_prereq() -> None:
    if not ENABLE_GOOGLE_DRIVE_UPLOAD:
        return
    if not SERVICE_ACCOUNT_JSON:
        raise FileNotFoundError("ไม่พบ Service Account JSON ใน ENV: SERVICE_ACCOUNT_JSON")
    if not DRIVE_FILE_ID:
        raise RuntimeError("ต้องตั้ง TMD_FILE_ID (fileId ของไฟล์ปลายทางบน Google Drive)")

def build_drive_service():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON), scopes=scopes
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_read_csv_as_df(service, file_id: str) -> Optional[pd.DataFrame]:
    try:
        req = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        content = fh.read()
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("utf-8", errors="replace")
        return pd.read_csv(StringIO(text))
    except HttpError as e:
        print(f"⚠️ อ่านไฟล์จาก Drive ไม่สำเร็จ: {e}")
        return None
    except Exception as e:
        print(f"⚠️ อ่าน CSV เป็น DataFrame ไม่สำเร็จ: {e}")
        return None

def drive_merge_and_update_df_update_only(
    df_new: pd.DataFrame,
    key_cols: Tuple[str, ...] = ("Province", "DateTime"),
    keep: str = "last",
    local_out_path: Optional[str] = None,
) -> Tuple[str, str, int]:
    _check_prereq()
    service = build_drive_service()

    # ตรวจไฟล์เดิม
    service.files().get(fileId=DRIVE_FILE_ID, fields="id,name,mimeType").execute()

    # โหลดไฟล์เดิม
    df_old = drive_read_csv_as_df(service, DRIVE_FILE_ID)
    if df_old is not None and len(df_old) > 0:
        common = [c for c in df_new.columns if c in df_old.columns]
        df_merged = (pd.concat([df_old[common], df_new[common]], ignore_index=True)
                     if common else pd.concat([df_old, df_new], ignore_index=True))
    else:
        df_merged = df_new.copy()

    # ทำให้ DateTime เป็นสตริงเสมอ (กันโดนพาร์สเพี้ยน)
    if "DateTime" in df_merged.columns:
        df_merged["DateTime"] = df_merged["DateTime"].astype(str)

    # ลบซ้ำตาม key โดยไม่แตะค่า DateTime เดิม
    effective_keys = [c for c in key_cols if c in df_merged.columns]
    if effective_keys:
        df_merged = df_merged.drop_duplicates(subset=effective_keys, keep=keep)

        # sort ด้วยคอลัมน์ช่วย แล้วลบทิ้ง
        if "DateTime" in df_merged.columns:
            sort_key = pd.to_datetime(df_merged["DateTime"], errors="coerce")
            df_merged = df_merged.assign(__sort_dt=sort_key).sort_values("__sort_dt").drop(columns="__sort_dt")
    else:
        df_merged = df_merged.drop_duplicates(keep=keep)

    # (ออปชัน) เขียนไฟล์โลคัล
    if local_out_path:
        df_merged.to_csv(local_out_path, index=False, encoding="utf-8-sig")

    # อัปเดตกลับไปที่ไฟล์เดิม (update only)
    from io import BytesIO
    from googleapiclient.http import MediaIoBaseUpload

    buf = BytesIO()
    buf.write(df_merged.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"))
    buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype=CSV_MIMETYPE, resumable=True)

    updated = service.files().update(
        fileId=DRIVE_FILE_ID,
        media_body=media,
        supportsAllDrives=True,
    ).execute()

    return "update", updated["id"], len(df_merged)

# ======================================================================
# SELENIUM HELPERS
# ======================================================================
def make_driver() -> webdriver.Chrome:
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1366,768")
    opt.page_load_strategy = PAGE_LOAD_STRATEGY
    drv = webdriver.Chrome(options=opt)   # Selenium Manager จะจัดการ chromedriver ให้อัตโนมัติ
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
    for _ in range(MAX_TRIES):
        try:
            sel = WebDriverWait(driver, WAIT_MED).until(
                EC.presence_of_element_located((By.ID, "province-selector"))
            )
            try:
                driver.execute_script("arguments[0].focus();arguments[0].click();", sel)
                time.sleep(0.2)
            except Exception:
                pass

            mapping: Dict[str, str] = {}
            for op in sel.find_elements(By.TAG_NAME, "option"):
                name = (op.text or "").strip()
                val = (op.get_attribute("value") or "").strip()
                if not name or not val or name.startswith("เลือก"):
                    continue
                mapping[name] = val

            if len(mapping) >= 10:
                return mapping
        except Exception:
            pass
        driver.refresh(); time.sleep(0.5)
    raise TimeoutException("อ่านรายชื่อจังหวัดได้น้อยผิดปกติ")

def _js_set_select_value(driver, value: str) -> bool:
    js = """
    var s=document.getElementById('province-selector');
    if(!s) return false;
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
    if ok: time.sleep(0.2)
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

    action, fid, merged_rows = "-", "-", 0
    if ENABLE_GOOGLE_DRIVE_UPLOAD and not new_df.empty:
        try:
            action, fid, merged_rows = drive_merge_and_update_df_update_only(
                new_df, key_cols=("Province", "DateTime"), keep="last", local_out_path=CSV_OUT
            )
            print(f"\n✅ อัปเดตไฟล์เดิมสำเร็จ (id={fid}), total rows after merge: {merged_rows}")
        except Exception as e:
            print("⚠️ Drive update fail:", e)
    else:
        if not new_df.empty:
            new_df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
            print(f"\n📝 บันทึกเฉพาะแถวใหม่ลงโลคอล: {CSV_OUT}")

    subject = f"[TMD Scraper] OK={len(all_rows)} FAIL={len(failed)}"
    body = (
        f"เพิ่มใหม่ (ก่อน merge): {len(all_rows)} แถว\n"
        f"รวมแล้วทั้งหมด (หลัง merge): {merged_rows or 0}\n"
        f"Drive: {action} id={fid}\n"
        f"Fail: {', '.join(failed) if failed else '-'}"
    )
    send_email(subject, body)

# ======================================================================
# ENTRY
# ======================================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f"[TMD Scraper] FAILED @ {when}"
        body = f"สคริปต์ล้มเหลวเมื่อ {when}\n\nError:\n{repr(e)}"
        send_email(subject, body)
        raise
