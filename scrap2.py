from __future__ import annotations

# ============================== 1) IMPORTS & CONFIG ==============================
import os
import re
import time
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------- Google Drive API (Service Account) --------
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from io import BytesIO

# ------------------------------- Runtime Config --------------------------------
URL: str = "https://nationalthaiwater.onwr.go.th/waterlevel"
CSV_OUT: str = r"C:\Project_End\CodeProject\waterlevel_report.csv"

# ----- Google Drive -----
ENABLE_GOOGLE_DRIVE_UPLOAD: bool = True
SERVICE_ACCOUNT_FILE: str = r"C:\Project_End\CodeProject\githubproject-467507-653192ee67bf.json"
DRIVE_FOLDER_ID: str = "1YV69Vah7gNvXbYZNKwjQyxQXT36MWjRH"
CSV_MIMETYPE: str = "text/csv"
DRIVE_FILE_ID: Optional[str] = "1zfUZeqp5qJrKxtgHu6XmqrN9omvaxVuP"

PAGE_TIMEOUT: int = 40
CLICK_TIMEOUT: int = 15
SLEEP_BETWEEN_PAGES: float = 1.0

# ================= Email Notify (SMTP) =================
EMAIL_ENABLED: bool = os.getenv("EMAIL_ENABLED", "true").lower() == "true"
SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT: int   = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER: str = os.getenv("EMAIL_SENDER", "pph656512@gmail.com")
EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "nfns uuan ayrx uykm")
EMAIL_TO: str      = os.getenv("EMAIL_TO", "pph656512@gmail.com")

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
        server.sendmail(EMAIL_SENDER, [x.strip() for x in EMAIL_TO.split(",")], msg.as_string())
        server.quit()
        print("📧 ส่งอีเมลแจ้งเตือนแล้ว")
    except Exception as e:
        print("⚠️ ส่งอีเมลล้มเหลว:", e)

# =========================== 2) Google Drive helpers ===========================
def _check_prereq() -> None:
    if not ENABLE_GOOGLE_DRIVE_UPLOAD:
        return
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"ไม่พบไฟล์ Service Account: {SERVICE_ACCOUNT_FILE}")
    if not DRIVE_FOLDER_ID:
        raise ValueError("ยังไม่ได้ตั้ง DRIVE_FOLDER_ID")
    if not DRIVE_FILE_ID_OVERRIDE:
        raise ValueError("ต้องตั้ง DRIVE_FILE_ID (DRIVE_FILE_ID_OVERRIDE) เป็น fileId ของไฟล์ปลายทาง")

def build_drive_service():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_read_csv_as_df(service, file_id: str) -> Optional[pd.DataFrame]:
    """ดาวน์โหลด CSV จาก Drive แล้วแปลงเป็น DataFrame (ถ้าไฟล์ว่าง คืน DataFrame ว่าง)"""
    try:
        req = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        data = fh.read()
        if not data:
            return pd.DataFrame()
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")
        if not text.strip():
            return pd.DataFrame()
        return pd.read_csv(pd.io.common.StringIO(text))
    except HttpError as e:
        print(f"⚠️ ดาวน์โหลดไฟล์จาก Drive ไม่สำเร็จ: {e}")
        return None
    except Exception as e:
        print(f"⚠️ อ่าน CSV เป็น DataFrame ไม่สำเร็จ: {e}")
        return None

def drive_merge_and_update_df_update_only(
    df_new: pd.DataFrame,
    key_cols: tuple[str, ...],
    local_out_path: Optional[str] = None,
) -> tuple[str, str, int]:
    """
    รวม df_new กับไฟล์เดิม (DRIVE_FILE_ID_OVERRIDE) แล้ว 'update' กลับไฟล์เดิมเท่านั้น
    - ไม่ค้นหา/ไม่สร้างใหม่ ถ้าเข้าถึงไฟล์เดิมไม่ได้ -> raise
    """
    _check_prereq()
    service = build_drive_service()

    # ตรวจสิทธิ์/มีอยู่จริงของไฟล์เดิม
    try:
        service.files().get(fileId=DRIVE_FILE_ID_OVERRIDE, fields="id,name").execute()
    except HttpError as e:
        raise RuntimeError(
            f"Service Account ไม่มีสิทธิ์หรือหาไฟล์ไม่พบ (fileId={DRIVE_FILE_ID_OVERRIDE})"
        ) from e

    # ดาวน์โหลดไฟล์เดิมมารวม
    df_old = drive_read_csv_as_df(service, DRIVE_FILE_ID_OVERRIDE)
    if df_old is None or df_old.empty:
        df_merged = df_new.copy()
    else:
        common = [c for c in df_new.columns if c in df_old.columns]
        df_merged = pd.concat([df_old[common], df_new[common] if common else df_new], ignore_index=True)

    # ลบซ้ำตาม key
    effective_keys = [c for c in key_cols if c in df_merged.columns]
    df_merged = df_merged.drop_duplicates(subset=effective_keys, keep="last") if effective_keys \
                else df_merged.drop_duplicates(keep="last")

    # บันทึกโลคอล (optional)
    if local_out_path:
        os.makedirs(os.path.dirname(local_out_path), exist_ok=True)
        df_merged.to_csv(local_out_path, index=False, encoding="utf-8-sig")

    # อัปเดตไฟล์เดิม (ต้องส่งเป็น bytes)
    buf = BytesIO()
    csv_bytes = df_merged.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    buf.write(csv_bytes); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype=CSV_MIMETYPE, resumable=True)

    updated = service.files().update(
        fileId=DRIVE_FILE_ID_OVERRIDE,
        media_body=media,
        supportsAllDrives=True,
    ).execute()

    return "update", updated["id"], len(df_merged)

# ============================= 3) Selenium scraper =============================
def make_driver() -> webdriver.Chrome:
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    drv = webdriver.Chrome(options=opt)
    drv.set_page_load_timeout(60)
    return drv

def scrape_waterlevel() -> tuple[list[list[str]], float]:
    driver = make_driver()
    start_time = time.time()
    try:
        driver.get(URL)
        rows = WebDriverWait(driver, PAGE_TIMEOUT).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".MuiTable-root tbody tr"))
        )
        all_data: list[list[str]] = []
        current_date = datetime.now().strftime("%m/%d/%y")
        while True:
            for row in rows:
                cols = [c.text.strip() for c in row.find_elements(By.CSS_SELECTOR, "td")]
                if len(cols) < 5:
                    continue
                # เติมคอลัมน์วันที่ท้ายตาราง
                if len(cols) == 9:
                    cols[-1] = current_date
                else:
                    cols.append(current_date)
                all_data.append(cols)
            try:
                next_btn = WebDriverWait(driver, CLICK_TIMEOUT).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[@title='Next Page']/button"))
                )
                if not next_btn.is_enabled():
                    break
                first_row_old = rows[0]
                driver.execute_script("arguments[0].click();", next_btn)
                WebDriverWait(driver, PAGE_TIMEOUT).until(EC.staleness_of(first_row_old))
                rows = WebDriverWait(driver, PAGE_TIMEOUT).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".MuiTable-root tbody tr"))
                )
                if SLEEP_BETWEEN_PAGES > 0:
                    time.sleep(SLEEP_BETWEEN_PAGES)
                print("➡️ Next Page Loaded")
            except Exception:
                break
        return all_data, start_time
    finally:
        driver.quit()

# ============================= 4) Text/Parsing helpers =============================
def extract_thai(text: str) -> str:
    if pd.isna(text) or text is None:
        return ""
    m = re.search(r"[ก-๙].*", str(text))
    return m.group(0).strip() if m else ""

# =============================== 5) Save & Upload ===============================
def save_and_upload(all_data: list[list[str]]) -> tuple[int, Optional[str], Optional[str]]:
    if not all_data:
        print("⚠️ ไม่พบข้อมูลให้บันทึก")
        return 0, None, None

    # ทำให้จำนวนคอลัมน์เท่ากัน
    max_cols = max(len(r) for r in all_data)
    all_data = [r + [""] * (max_cols - len(r)) for r in all_data]

    # ✅ ใช้คอลัมน์ภาษาอังกฤษเท่านั้น
    headers = [
        "Station","Location","Time","Water_Level","Bank_Level",
        "Gauge_Zero","Capacity_Percent","Status","Data_Time",
    ]
    if len(headers) < max_cols:
        headers += [f"Extra_{i + 1}" for i in range(max_cols - len(headers))]

    df_new = pd.DataFrame(all_data, columns=headers)
    # ถ้าค่าใน Station เป็นภาษาไทย ให้ดึงเฉพาะส่วนตัวอักษรไทยออกมา (เลือกจะคงไว้ก็ได้)
    df_new["Station"] = df_new["Station"].apply(extract_thai)

    drive_action = None
    drive_file_id = None
    merged_rows = 0

    if ENABLE_GOOGLE_DRIVE_UPLOAD:
        try:
            # ✅ คีย์ลบซ้ำเป็นภาษาอังกฤษ
            key_cols = ("Station", "Time", "Data_Time")
            drive_action, drive_file_id, merged_rows = drive_merge_and_update_df_update_only(
                df_new=df_new,
                key_cols=key_cols,
                local_out_path=CSV_OUT,   # เก็บไฟล์รวมล่าสุดไว้โลคอลด้วย
            )
            print(f"✅ รวม+อัปเดตไฟล์เดิมสำเร็จ: action={drive_action}, id={drive_file_id}, rows={merged_rows}")
            return merged_rows, drive_action, drive_file_id
        except Exception as e:
            print("⚠️ อัปเดตกลับ Drive ล้มเหลว:", e)
            df_new.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
            return len(df_new), None, None
    else:
        df_new.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
        print(f"💾 บันทึก {len[df_new]} แถว -> {os.path.abspath(CSV_OUT)}")
        return len(df_new), None, None

# ==================================== 6) Main ====================================
def main() -> None:
    all_data, t0 = scrape_waterlevel()
    rows_saved, drive_action, drive_file_id = save_and_upload(all_data)
    elapsed = time.time() - t0

    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[WaterLevel] Finish OK rows={rows_saved} @ {when}"
    body = (
        f"สถานะการรันสคริปต์ ({when})\n"
        f"- บันทึก/รวมแล้ว: {rows_saved} แถว\n"
        f"- ไฟล์ CSV: {os.path.abspath(CSV_OUT)}\n"
        f"- ใช้เวลา: {elapsed:.2f} วินาที\n"
    )
    if ENABLE_GOOGLE_DRIVE_UPLOAD:
        body += f"- Drive: {drive_action or '-'} (id={drive_file_id or '-'})\n"

    send_email(subject, body)
    print(f"⏱ ใช้เวลาในการรันทั้งหมด: {elapsed:.2f} วินาที")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f"[WaterLevel] FAILED @ {when}"
        body = f"สคริปต์ล้มเหลวเมื่อ {when}\n\nError:\n{repr(e)}"
        send_email(subject, body)
        raise
