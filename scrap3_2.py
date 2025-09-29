# -*- coding: utf-8 -*-
import os, time, json
import pandas as pd
from pathlib import Path
from datetime import datetime
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# ============== I/O (ไฟล์เข้า/ออก) ============== #
LARGE_CSV  = Path(r"C:\Project_End\CodeProject\waterdam_report_large.csv").resolve()
MEDIUM_CSV = Path(r"C:\Project_End\CodeProject\waterdam_report_medium.csv").resolve()
OUT_CSV    = Path(r"C:\Project_End\CodeProject\waterdam_report.csv").resolve()

# ============== Upload to Google Drive ============== #
ENABLE_GOOGLE_DRIVE_UPLOAD = True
SERVICE_ACCOUNT_FILE = Path(r"C:\Project_End\CodeProject\githubproject-467507-653192ee67bf.json").resolve()
DRIVE_FOLDER_ID = "1YV69Vah7gNvXbYZNKwjQyxQXT36MWjRH"
CSV_MIMETYPE = "text/csv"
DRIVE_FILE_ID_OVERRIDE: str = os.getenv("DRIVE_FILE_ID") or "1xe7-Yy_wL7TgHpmfLr5vFAHkesvtzloC"

# ============== Email Notify (SMTP) ============== #
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "true").lower() == "true"
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER  = os.getenv("EMAIL_SENDER", "pph656512@gmail.com")
EMAIL_PASS    = os.getenv("EMAIL_PASSWORD", "nfns uuan ayrx uykm")
EMAIL_TO      = os.getenv("EMAIL_TO", "pph656512@gmail.com")

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
        server.login(EMAIL_SENDER, EMAIL_PASS)
        server.sendmail(EMAIL_SENDER, [x.strip() for x in EMAIL_TO.split(",")], msg.as_string())
        server.quit()
        print("📧 ส่งอีเมลแจ้งเตือนแล้ว")
    except Exception as e:
        print("⚠️ ส่งอีเมลล้มเหลว:", e)

# ================= Drive Helpers ================= #
def _build_drive_service_with_service_account():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_or_update_csv(local_path, drive_folder_id, target_name=None, max_retries=3):
    local_path = Path(local_path).resolve()
    if target_name is None:
        target_name = local_path.name
    if not local_path.exists():
        raise FileNotFoundError(f"ไม่พบไฟล์ที่จะอัปโหลด: {local_path}")

    service = _build_drive_service_with_service_account()
    media = MediaFileUpload(str(local_path), mimetype=CSV_MIMETYPE, resumable=True)

    # ใช้ fileId เดิม (ถ้ามี)
    file_id = DRIVE_FILE_ID_OVERRIDE
    if file_id:
        try:
            updated = service.files().update(
                fileId=file_id, media_body=media, supportsAllDrives=True
            ).execute()
            print(f"☁️ อัปเดตไฟล์สำเร็จ (id={updated.get('id')})")
            return "update", updated.get("id")
        except HttpError as e:
            print(f"⚠️ อัปเดตไฟล์ไม่สำเร็จ: {e}")

    # ถ้าไม่มี -> สร้างใหม่
    file_metadata = {"name": target_name, "parents": [drive_folder_id]}
    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,webViewLink",
        supportsAllDrives=True,
    ).execute()
    new_id = created.get("id")
    print(f"☁️ อัปโหลดไฟล์ใหม่สำเร็จ (id={new_id})")
    return "create", new_id

# ======================== Core (รวมไฟล์ + Clean) ======================== #
def read_csv_smart(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)

def run_merge_only():
    df_large  = read_csv_smart(LARGE_CSV)
    df_medium = read_csv_smart(MEDIUM_CSV)

    # ให้คอลัมน์ตรงกัน
    ordered_cols = list(df_large.columns)
    for c in ordered_cols:
        if c not in df_medium.columns:
            df_medium[c] = pd.NA
    df_medium = df_medium[ordered_cols]

    df_large["DamType"]  = "large"
    df_medium["DamType"] = "medium"

    df = pd.concat([df_large, df_medium], ignore_index=True)

    # ✅ แทนค่า missing ให้เป็น "0"
    df = df.fillna("0")
    df = df.replace({"-": "0", "--": "0", "–": "0", "—": "0", "": "0"})

    # ลบแถวซ้ำ
    df = df.drop_duplicates()

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"💾 รวมไฟล์แล้ว: {OUT_CSV} ({len(df):,} แถว)")

    drive_action = drive_id = None
    if ENABLE_GOOGLE_DRIVE_UPLOAD:
        try:
            drive_action, drive_id = drive_upload_or_update_csv(OUT_CSV, DRIVE_FOLDER_ID, OUT_CSV.name)
            print(f"✅ Drive: {drive_action} (id={drive_id})")
        except Exception as e:
            print(f"⚠️ อัปโหลด Drive ล้มเหลว: {e}")

    return len(df), str(OUT_CSV), drive_action, drive_id

# ================================== MAIN ================================== #
def main():
    t0 = time.time()
    rows, out_path, drive_action, drive_id = run_merge_only()
    elapsed = time.time() - t0
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    subject = f"[WaterDam MERGE] OK rows={rows} @ {when}"
    body = (
        f"รวมไฟล์เสร็จแล้ว\n"
        f"- แถว: {rows}\n"
        f"- ไฟล์: {out_path}\n"
        f"- ใช้เวลา: {elapsed:.2f} วินาที\n"
    )
    if ENABLE_GOOGLE_DRIVE_UPLOAD:
        body += f"- Drive: {drive_action or '-'} (id={drive_id or '-'})\n"

    send_email(subject, body)

if __name__ == "__main__":
    main()
