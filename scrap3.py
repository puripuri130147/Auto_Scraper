import os
import time
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ================================== CONFIG ================================== #
URL = "https://nationalthaiwater.onwr.go.th/dam"

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# -------- Email --------
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "true").lower() == "true"
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")   # PSU: smtp.office365.com
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER  = os.getenv("EMAIL_SENDER", "pph656512@gmail.com")
EMAIL_PASS    = os.getenv("EMAIL_PASSWORD", "nfns uuan ayrx uykm")              # Gmail: App Password / PSU: password
EMAIL_TO      = os.getenv("EMAIL_TO", "pph656512@gmail.com")

def send_email(subject: str, body: str):
    if not EMAIL_ENABLED: 
        return
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_TO
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASS)
        server.sendmail(EMAIL_SENDER, [EMAIL_TO], msg.as_string())
        server.quit()
        print("📧 ส่งอีเมลแล้ว")
    except Exception as e:
        print("⚠️ ส่งอีเมลล้มเหลว:", e)

# ================================== FUNCTIONS ================================== #
def scrape_data(tab_name: str) -> list[list[str]]:
    all_data = []
    current_date = datetime.today().strftime("%m/%d/%Y")
    page = 1
    print(f"\nเริ่มดึงข้อมูล: {tab_name}")
    while True:
        time.sleep(2)
        rows = driver.find_elements(By.CSS_SELECTOR, ".MuiTable-root tbody tr")
        count_before = len(all_data)
        for row in rows:
            cols = [col.text.strip() for col in row.find_elements(By.CSS_SELECTOR, "td")]
            if any(col not in ("", "-", None) for col in cols):
                cols += [current_date, tab_name]
                all_data.append(cols)
        scraped_this_page = len(all_data) - count_before
        print(f"หน้า {page}: เก็บข้อมูลแล้ว {scraped_this_page} แถว")
        try:
            next_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//span[@title='Next Page']/button"))
            )
            if next_button.is_enabled():
                driver.execute_script("arguments[0].click();", next_button)
                page += 1
                print(f"ไปยังหน้า {page}...")
                time.sleep(2)
            else:
                print(f"จบการดึงข้อมูล: {tab_name}")
                break
        except:
            print(f"ไม่พบปุ่ม 'Next Page' หรือคลิกไม่ได้: {tab_name}")
            break
    return all_data

def save_data_to_csv(data: list[list[str]], dam_type: str) -> int:
    if not data:
        print(f"⚠️ ไม่มีข้อมูล {dam_type} ให้บันทึก")
        return 0
    file_path = f"waterdam_report_{dam_type}.csv"
    file_exists = os.path.exists(file_path)
    df = pd.DataFrame(data)
    df.replace("", pd.NA, inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    if file_exists:
        with open(file_path, encoding="utf-8-sig") as f:
            first_line = f.readline()
            existing_cols = len(first_line.strip().split(",")) if first_line else 0
        if existing_cols and existing_cols != df.shape[1]:
            print(f"⚠️ โครงสร้างไม่ตรงกับไฟล์เดิม ไม่บันทึก {dam_type}")
            return 0
    df.to_csv(file_path, mode="a", index=False, encoding="utf-8-sig", header=not file_exists)
    print(f"💾 บันทึกข้อมูล {dam_type} ลงไฟล์ {file_path} แล้ว ({len(df)} แถว)")
    return len(df)

# ================================== MAIN ================================== #
if __name__ == "__main__":
    start_time = time.time()
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(URL)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".MuiTable-root tbody tr"))
        )
        large_dam_data = scrape_data("แหล่งน้ำขนาดใหญ่")
        medium_tab_button = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//button[@aria-controls='tabpanel-1']"))
        )
        try:
            WebDriverWait(driver, 10).until_not(
                EC.presence_of_element_located((By.CLASS_NAME, "MuiBackdrop-root"))
            )
        except: pass
        driver.execute_script("arguments[0].click();", medium_tab_button)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".MuiTable-root tbody tr"))
        )
        medium_dam_data = scrape_data("แหล่งน้ำขนาดกลาง")
        rows_large = save_data_to_csv(large_dam_data, "large")
        rows_medium = save_data_to_csv(medium_dam_data, "medium")
        elapsed = time.time() - start_time
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f"[WaterDam] Finish OK large={rows_large} medium={rows_medium} @ {when}"
        body = (
            f"รันสำเร็จ {when}\n"
            f"- Large: {rows_large} แถว\n"
            f"- Medium: {rows_medium} แถว\n"
            f"- ใช้เวลา: {elapsed:.2f} วินาที\n"
        )
        send_email(subject, body)
    except Exception as e:
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        send_email(f"[WaterDam] FAILED @ {when}", f"Error: {repr(e)}")
        raise
    finally:
        driver.quit()
