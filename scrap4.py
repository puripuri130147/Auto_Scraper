# -*- coding: utf-8 -*-
import os, time, random, re, json, hashlib, shutil
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse

URL = "http://app.dgr.go.th/newpasutara/xml/search.php"
OUT_DIR = r"C:\Project_End\CodeProject\dgr_results"
ALL_PATH = os.path.join(OUT_DIR, "dgr_all_provinces.csv")
UPLOAD_STATE_PATH = os.path.join(OUT_DIR, "dgr_upload_state.json")

TYPE_MAP = {
    "w1.png": "บ่ออุปโภค-บริโภค",
    "w2.png": "บ่อเกษตร",
}

RUN_ID = datetime.now().strftime("%Y%m%d-%H%M%S")
RUN_STARTED_AT = datetime.now().isoformat(timespec="seconds")
RUN_LOG_PATH = os.path.join(OUT_DIR, "dgr_run_log.csv")
SESSION_SUMMARY_PATH = os.path.join(OUT_DIR, f"dgr_session_{RUN_ID}.json")

# Google Drive
ENABLE_GOOGLE_DRIVE_UPLOAD = True
SERVICE_ACCOUNT_FILE = r"C:/Project_End/CodeProject/githubproject-467507-653192ee67bf.json"
DRIVE_FOLDER_ID = "1YV69Vah7gNvXbYZNKwjQyxQXT36MWjRH"
CSV_MIMETYPE = "text/csv"
DRIVE_FILE_ID_OVERRIDE = os.getenv("DRIVE_FILE_ID") or "1OUDOHzE6u3J6Bo4TMmthNlZ1Hw9Wh3I4"  

# ---------- Column Rename: TH -> EN ----------
RENAME_EXACT = {
    "จังหวัด": "Province",
    "ลำดับ": "No",
    "รหัสบ่อ": "WellID",
    "สถานที่ตั้ง": "Location",
    "ประเภท": "Type",
    "ประเภทบ่อ": "Type",  # กรณีสคริปต์ตั้งหัวเป็น "ประเภทบ่อ"
    "ความลึก (เมตร)": "Depth",
    "ปริมาณน้ำ (เมตร³/ชม.)": "Flow",
    "ระดับน้ำปกติ (เมตร)": "NormalLevel",
    "ระยะน้ำลด (เมตร)": "Drop",
    "น้ำต้นทุน (เมตร³/วัน.)": "Capacity",
    "วันที่เก็บข้อมูล": "CollectedAt",
}

# เผื่อกรณีหัวคอลัมน์มีความต่างเล็กน้อยเรื่องจุด/เว้นวรรค/สัญลักษณ์
RENAME_PATTERNS = [
    (re.compile(r"^\s*ความลึก\s*\(เมตร\)\s*$"), "Depth"),
    (re.compile(r"^\s*ปริมาณน้ำ\s*\(เมตร.?3\s*/\s*ชม\.?\)\s*$"), "Flow"),
    (re.compile(r"^\s*น้ำต้นทุน\s*\(เมตร.?3\s*/\s*วัน\.?\)\s*$"), "Capacity"),
    (re.compile(r"^\s*ระดับน้ำปกติ\s*\(เมตร\)\s*$"), "NormalLevel"),
    (re.compile(r"^\s*ระยะน้ำลด\s*\(เมตร\)\s*$"), "Drop"),
]

def _normalize_colname(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())

def rename_th_to_en(df: pd.DataFrame) -> pd.DataFrame:
    # 1) exact mapping ก่อน
    if df is None or df.empty:
        return df
    df = df.rename(columns={k: v for k, v in RENAME_EXACT.items() if k in df.columns})

    # 2) pattern mapping
    renamed = {}
    for col in df.columns:
        if col in RENAME_EXACT.values():
            continue
        c_norm = _normalize_colname(col)
        for pat, newname in RENAME_PATTERNS:
            if pat.match(c_norm):
                renamed[col] = newname
                break
    if renamed:
        df = df.rename(columns=renamed)

    # 3) จัดลำดับใหม่นำหน้าเป็น Province, CollectedAt (ถ้ามี)
    front = [c for c in ["Province", "CollectedAt"] if c in df.columns]
    others = [c for c in df.columns if c not in front]
    return df[front + others] if front else df

def migrate_existing_csv_headers(csv_path: str):
    """แปลงหัวคอลัมน์ไฟล์เก่า (ถ้ามี) ให้เป็นอังกฤษครั้งเดียว และสำรองไฟล์ .bak"""
    if not os.path.exists(csv_path):
        return
    try:
        _df = pd.read_csv(csv_path)
        new_df = rename_th_to_en(_df.copy())
        if list(new_df.columns) != list(_df.columns):
            bak = f"{csv_path}.bak.{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            shutil.copy2(csv_path, bak)
            new_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"🔁 Migrated headers to EN (backup: {bak})")
    except Exception as e:
        print(f"⚠️ migrate headers skipped: {e}")

# ---------- Utils ----------
def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def format_secs(sec: float) -> str:
    if sec < 60: return f"{sec:.2f}s"
    m, s = divmod(sec, 60)
    if m < 60: return f"{int(m)}m {s:.1f}s"
    h, m = divmod(int(m), 60)
    return f"{h}h {m}m {s:.0f}s"

def extract_cell_text(td):
    imgs = td.find_elements(By.TAG_NAME, "img")
    if imgs:
        src = imgs[0].get_attribute("src") or ""
        filename = os.path.basename(urlparse(src).path)
        if filename in TYPE_MAP: return TYPE_MAP[filename]
        alt = (imgs[0].get_attribute("alt") or "").strip()
        if alt: return alt
    return clean_text(td.get_attribute("textContent"))

def normalize_headers(headers):
    out = [clean_text(h) for h in headers]
    for i, h in enumerate(out):
        if not h: out[i] = f"col_{i+1}"
    return out

def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    empty_cols = [c for c in df.columns if df[c].replace("", pd.NA).isna().all()]
    return df.drop(columns=empty_cols)

def table_to_dataframe(table_el):
    ths = table_el.find_elements(By.CSS_SELECTOR, "thead th")
    headers = normalize_headers([th.get_attribute("textContent") for th in ths])

    rows = []
    for tr in table_el.find_elements(By.CSS_SELECTOR, "tbody tr"):
        cells = tr.find_elements(By.CSS_SELECTOR, "th,td")
        rows.append([extract_cell_text(td) for td in cells])

    if not rows:
        return pd.DataFrame()

    max_len = max(len(r) for r in rows)
    if len(headers) != max_len:
        headers = (headers + [f"col_{i+1}" for i in range(len(headers), max_len)])[:max_len]
    rows = [r + [""]*(max_len - len(r)) for r in rows]
    df = pd.DataFrame(rows, columns=headers)

    df = drop_empty_columns(df)

    # ตัดบรรทัด "ค่าเฉลี่ย" ทุกคอลัมน์
    if not df.empty:
        mask_avg = pd.Series(False, index=df.index)
        for c in df.columns:
            mask_avg |= df[c].astype(str).str.contains(r"ค่าเฉลี่ย", na=False)
        df = df[~mask_avg].reset_index(drop=True)

    # รีเนมคอลัมน์ประเภทบ่อถ้า pattern ตรง TYPE_MAP
    type_values = set(TYPE_MAP.values())
    for c in df.columns:
        vals = set(v for v in df[c].unique() if isinstance(v, str) and v)
        if vals and vals.issubset(type_values) and (c.startswith("col_") or not c):
            df = df.rename(columns={c: "ประเภทบ่อ"})
            break
    return df

def scroll_container_load_all(driver, container, pause=0.6, max_rounds=40):
    last_count, stable = -1, 0
    for _ in range(max_rounds):
        try:
            count = len(container.find_element(By.TAG_NAME, "tbody").find_elements(By.TAG_NAME, "tr"))
        except Exception:
            count = 0
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
        time.sleep(pause)
        if count == last_count:
            stable += 1
            if stable >= 2: break
        else:
            stable = 0
            last_count = count

def find_next_button(driver):
    css_candidates = [
        "a.paginate_button.next:not(.disabled)",
        "li.paginate_button.next:not(.disabled) a",
        "li.page-item.next:not(.disabled) a.page-link",
        "ul.pagination li.next:not(.disabled) a",
        "a[aria-label='Next']:not(.disabled)",
    ]
    for sel in css_candidates:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if el.is_displayed() and el.is_enabled():
                return el
        except Exception:
            pass
    try:
        return driver.find_element(By.XPATH, "//a[contains(., 'ถัดไป') or contains(., 'Next')][not(contains(@class,'disabled'))]")
    except Exception:
        return None

def collect_table_all_pages(driver, wait):
    t0 = time.perf_counter()
    container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#myTable.table-responsive")))
    driver.set_window_size(1920, 1400)

    scroll_container_load_all(driver, container)
    table_el = container.find_element(By.TAG_NAME, "table")
    frames = [table_to_dataframe(table_el)]

    while True:
        nxt = find_next_button(driver)
        if not nxt: break
        old_tbody = container.find_element(By.TAG_NAME, "tbody")
        nxt.click()
        wait.until(EC.staleness_of(old_tbody))
        container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#myTable.table-responsive")))
        scroll_container_load_all(driver, container)
        table_el = container.find_element(By.TAG_NAME, "table")
        frames.append(table_to_dataframe(table_el))
        time.sleep(random.uniform(0.4, 0.8))

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, (time.perf_counter() - t0)

# ---- append_save: ต่อท้ายเสมอ + ลบคอลัมน์ run_id / fetched_at ออกจากไฟล์เก่า (ถ้ามี) ----
def append_save(df_new: pd.DataFrame, csv_path: str) -> int:
    exists = os.path.exists(csv_path)
    if exists:
        try:
            sample = pd.read_csv(csv_path, nrows=0)
            # one-time migration: drop columns we no longer use
            drop_cols = [c for c in ["run_id", "fetched_at"] if c in sample.columns]
            if drop_cols:
                df_old = pd.read_csv(csv_path)
                df_old = df_old.drop(columns=drop_cols)
                df_old.to_csv(csv_path, index=False, encoding="utf-8-sig")
                sample = pd.read_csv(csv_path, nrows=0)  # refresh header

            cols = list(sample.columns)
            for c in df_new.columns:
                if c not in cols:
                    cols.append(c)
            df_new = df_new.reindex(columns=cols)
        except Exception:
            pass

    df_new.to_csv(csv_path, mode="a", header=not exists, index=False, encoding="utf-8-sig")
    return len(df_new)

def ensure_outdir(): os.makedirs(OUT_DIR, exist_ok=True)

def write_run_log(row: dict):
    ensure_outdir()
    df = pd.DataFrame([row])
    df.to_csv(RUN_LOG_PATH, mode="a", header=not os.path.exists(RUN_LOG_PATH), index=False, encoding="utf-8-sig")

def save_session_summary(summary: dict):
    ensure_outdir()
    with open(SESSION_SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

# === Google Drive (ขั้นต่ำที่จำเป็น) ===
def _build_drive_service_with_service_account():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_find_file_in_folder(service, filename, folder_id):
    from googleapiclient.errors import HttpError
    safe_name = filename.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    try:
        res = service.files().list(
            q=q, fields="files(id, name)", includeItemsFromAllDrives=True, supportsAllDrives=True
        ).execute()
        return res.get("files", [])
    except HttpError as e:
        raise RuntimeError(f"ค้นหาไฟล์บน Drive ล้มเหลว: {e}")

def drive_upload_or_update_csv(service, local_path, drive_folder_id, target_name=None, max_retries=3):
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError

    if target_name is None:
        target_name = os.path.basename(local_path)

    # ยืนยันสิทธิ์เข้าถึงโฟลเดอร์
    service.files().get(fileId=drive_folder_id, fields="id", supportsAllDrives=True).execute()

    media = MediaFileUpload(local_path, mimetype=CSV_MIMETYPE, resumable=True)

    # ---------- 1) ใช้ fileId จาก ENV ----------
    file_id = DRIVE_FILE_ID_OVERRIDE
    if file_id:
        for attempt in range(1, max_retries + 1):
            try:
                r = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
                _set_cached_file_id(r.get("id"))
                return ("update", r.get("id"))
            except HttpError as e:
                if attempt == max_retries:
                    break
                time.sleep(2 * attempt)

    # ---------- 2) ใช้ fileId จาก cache (state) ----------
    file_id = _get_cached_file_id()
    if file_id:
        for attempt in range(1, max_retries + 1):
            try:
                r = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
                return ("update", r.get("id"))
            except HttpError:
                if attempt == max_retries:
                    break
                time.sleep(2 * attempt)

    # ---------- 3) ค้นจากชื่อไฟล์ในโฟลเดอร์ ----------
    exists = drive_find_file_in_folder(service, target_name, drive_folder_id)
    if exists:
        file_id = exists[0]["id"]
        for attempt in range(1, max_retries + 1):
            try:
                r = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
                _set_cached_file_id(r.get("id"))
                return ("update", r.get("id"))
            except HttpError:
                if attempt == max_retries:
                    raise
                time.sleep(2 * attempt)

    # ---------- 4) ไม่พบ -> สร้างใหม่ + cache ----------
    meta = {"name": target_name, "parents": [drive_folder_id]}
    for attempt in range(1, max_retries + 1):
        try:
            r = service.files().create(
                body=meta, media_body=media, fields="id,webViewLink", supportsAllDrives=True
            ).execute()
            _set_cached_file_id(r.get("id"))
            return ("create", r.get("id"))
        except HttpError:
            if attempt == max_retries:
                raise
            time.sleep(2 * attempt)

def _sha1(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""): h.update(chunk)
    return h.hexdigest()

def _load_upload_state() -> dict:
    if os.path.exists(UPLOAD_STATE_PATH):
        try:
            with open(UPLOAD_STATE_PATH, "r", encoding="utf-8") as f: return json.load(f)
        except Exception:
            return {}
    return {}

def _save_upload_state(state: dict):
    with open(UPLOAD_STATE_PATH, "w", encoding="utf-8") as f: json.dump(state, f, ensure_ascii=False, indent=2)
    
def _get_cached_file_id() -> str | None:
    st = _load_upload_state()
    return st.get("all_csv_file_id")

def _set_cached_file_id(file_id: str) -> None:
    st = _load_upload_state()
    st["all_csv_file_id"] = file_id
    _save_upload_state(st)

# === Main ===
def run_all_provinces(headless=True):
    ensure_outdir()

    options = webdriver.ChromeOptions()
    if headless: options.add_argument("--headless=new")
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1400")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 25)

    total_start = time.perf_counter()
    done_count = 0; per_prov_times = []
    session_added_total = 0; session_rows_total = 0; session_errors = []

    try:
        driver.get(URL)
        select = Select(wait.until(EC.presence_of_element_located((By.ID, "country-dropdown"))))
        provinces = [(opt.get_attribute("value").strip(), clean_text(opt.text)) for opt in select.options]
        provinces = [(v, t) for v, t in provinces if v]
        print(f"พบจังหวัดทั้งหมด {len(provinces)} จังหวัด")

        for i, (value, label) in enumerate(provinces, 1):
            prov_start = time.perf_counter()
            print(f"\n[{i}/{len(provinces)}] ดึงข้อมูล: {label}")
            status, error_msg = "success", ""
            prov_rows = added_this = 0; collect_secs = 0.0

            try:
                select.select_by_value(value)
                driver.find_element(By.CSS_SELECTOR,"button.btn.btn-primary[type='submit'], button.btn.btn-primary").click()

                df, collect_secs = collect_table_all_pages(driver, wait)
                prov_rows = len(df)
                if df.empty:
                    print(f"  ⚠️ ตารางว่างของ {label} ({format_secs(collect_secs)})")
                else:
                    # 1) เพิ่มจังหวัด + เวลาเก็บข้อมูล (ไทย)
                    df.insert(0, "จังหวัด", label)
                    df.insert(1, "วันที่เก็บข้อมูล", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                    # 2) รีเนมไทย -> อังกฤษ ตามที่กำหนด
                    df = rename_th_to_en(df)

                    # 3) one-time: แปลงหัวไฟล์เก่าให้เป็นอังกฤษก่อน append (กันหัวปน)
                    migrate_existing_csv_headers(ALL_PATH)

                    # 4) append save
                    added_this = append_save(df, ALL_PATH)
                    print(f"  ✅ บันทึกลงไฟล์รวม (+{added_this} แถว) | {format_secs(collect_secs)}")
                    session_added_total += added_this; session_rows_total += prov_rows
                done_count += 1

            except Exception as e:
                status, error_msg = "error", str(e)
                session_errors.append({"province": label, "error": error_msg})
                print(f"  ❌ {label}: {e}")

            prov_dur = time.perf_counter() - prov_start
            per_prov_times.append(prov_dur)
            write_run_log({
                "run_id": RUN_ID, "started_at": RUN_STARTED_AT, "province": label, "province_value": value,
                "rows_collected": prov_rows, "rows_appended_all_file": added_this,
                "collect_secs": round(collect_secs, 3), "duration_secs": round(prov_dur, 3),
                "status": status, "error": error_msg
            })

            # กลับหน้าเลือกจังหวัด
            driver.get(URL)
            select = Select(wait.until(EC.presence_of_element_located((By.ID, "country-dropdown"))))
            time.sleep(random.uniform(0.8, 1.5))

        total_dur = time.perf_counter() - total_start
        avg = (sum(per_prov_times) / len(per_prov_times)) if per_prov_times else 0.0
        print("\n🎉 เสร็จสิ้นเก็บทุกจังหวัด")
        print(f"⏱ รวม: {format_secs(total_dur)} | เฉลี่ย/จังหวัด: {format_secs(avg)} | สำเร็จ {done_count}/{len(provinces)}")

        save_session_summary({
            "run_id": RUN_ID, "started_at": RUN_STARTED_AT, "finished_at": datetime.now().isoformat(timespec="seconds"),
            "provinces_total": len(provinces), "provinces_done": done_count,
            "session_rows_collected": session_rows_total, "session_rows_appended": session_added_total,
            "avg_secs_per_province": round(avg, 3), "duration_secs_total": round(total_dur, 3), "errors": session_errors,
        })

        # อัปโหลดครั้งเดียว + skip ถ้าไฟล์ไม่เปลี่ยน
        if ENABLE_GOOGLE_DRIVE_UPLOAD and os.path.exists(ALL_PATH):
            try:
                new_sha1 = _sha1(ALL_PATH)
                state = _load_upload_state(); old_sha1 = state.get("all_csv_sha1")
                if old_sha1 == new_sha1:
                    print("☁️ ข้ามอัปโหลด Google Drive (ไฟล์ไม่เปลี่ยน)")
                else:
                    service = _build_drive_service_with_service_account()
                    action, file_id = drive_upload_or_update_csv(service, ALL_PATH, DRIVE_FOLDER_ID, os.path.basename(ALL_PATH))
                    print(f"☁️ {'อัปเดต' if action=='update' else 'อัปโหลดใหม่'} ไปยัง Google Drive (fileId={file_id})")
                    state["all_csv_sha1"] = new_sha1; _save_upload_state(state)
            except Exception as e:
                print(f"⚠️ อัปโหลด Google Drive ล้มเหลว: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_all_provinces(headless=True)
