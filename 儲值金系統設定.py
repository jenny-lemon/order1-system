# -*- coding: utf-8 -*-
import re
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from accounts import ACCOUNTS
from env import (
    ENV,
    BASE_URL_DEV,
    BASE_URL_PROD,
    GOOGLE_SHEET_ID,
    ENABLE_GCAL_COLOR_SYNC,
    GOOGLE_CALENDAR_MAP,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    COLOR_PURPLE,
    COLOR_YELLOW,
    REQUEST_DELAY,
    ORDER_PREFIX_DEV,
    ORDER_PREFIX_PROD,
)

if ENV == "dev":
    BASE_URL = BASE_URL_DEV
    ORDER_PREFIX = ORDER_PREFIX_DEV
else:
    BASE_URL = BASE_URL_PROD
    ORDER_PREFIX = ORDER_PREFIX_PROD

LOGIN_URL = f"{BASE_URL}/login"
BOOKING_URL = f"{BASE_URL}/booking/stored_value_routine"
PURCHASE_URL = f"{BASE_URL}/purchase"
GET_MEMBER_URL = f"{BASE_URL}/ajax/get_member"
CHECK_CONTAIN_URL = f"{BASE_URL}/ajax/check_contain"
CALCULATE_HOUR_URL = f"{BASE_URL}/ajax/calculate_hour"
GET_SECTION_URL = f"{BASE_URL}/ajax/get_section"
MAIL_SUCCESS_URL = f"{BASE_URL}/purchase/mail_success/{{order_no}}"

HEADERS = {"User-Agent": "Mozilla/5.0"}
MAIL_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Referer": PURCHASE_URL,
}

CLEAN_TYPE_MAP = {"居家清潔": "1", "辦公室清潔": "2", "裝修細清": "3"}
ORDER_NO_REGEX = r"(LC|TT)\d+"

STANDARD_SLOTS = [
    "08:30-12:30",
    "09:00-11:00",
    "09:00-12:00",
    "14:00-16:00",
    "14:00-17:00",
    "14:00-18:00",
    "09:00-16:00",
    "09:00-18:00",
]

def is_blank(value):
    return str(value).strip() in ("", "nan", "None")

def normalize_phone(phone_value):
    phone = str(phone_value).strip().replace(".0", "")
    phone = re.sub(r"\D", "", phone)
    if len(phone) == 9:
        phone = "0" + phone
    return phone

def parse_date_value(date_value):
    if isinstance(date_value, pd.Timestamp):
        return date_value.to_pydatetime()
    text = str(date_value).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise Exception(f"無法解析日期: {date_value}")

def get_date_str(date_value):
    return parse_date_value(date_value).strftime("%Y-%m-%d")

def is_weekend(date_value):
    return parse_date_value(date_value).weekday() >= 5

def get_unit_price_by_date(date_value):
    return 700 if is_weekend(date_value) else 600

def parse_time_slot(start_time_str, end_time_str):
    if not str(start_time_str).strip() or not str(end_time_str).strip():
        raise Exception(f"開始時間或結束時間為空：{start_time_str} / {end_time_str}")
    def to_hm(t):
        text = str(t).strip()
        parts = text.split(":")
        if not parts or not parts[0].strip():
            raise Exception(f"時間格式錯誤：{t}")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 and parts[1].strip() else 0
        return h, m
    sh, sm = to_hm(start_time_str)
    eh, em = to_hm(end_time_str)
    return sh, sm, eh, em

def calc_hours_from_time(start_time_str, end_time_str):
    sh, sm, eh, em = parse_time_slot(start_time_str, end_time_str)
    hours = (eh - sh) + (em - sm) / 60.0
    return hours if hours > 0 else None

def calc_effective_hours_from_time(start_time_str, end_time_str):
    hours = calc_hours_from_time(start_time_str, end_time_str)
    if hours is None:
        return None
    if hours >= 7:
        hours -= 1
    return hours

def normalize_period_text(start_time_str, end_time_str):
    sh, sm, eh, em = parse_time_slot(start_time_str, end_time_str)
    return f"{sh:02d}:{sm:02d}-{eh:02d}:{em:02d}"

def display_period_text(start_time_str, end_time_str):
    sh, sm, eh, em = parse_time_slot(start_time_str, end_time_str)
    return f"{sh:02d}:{sm:02d} - {eh:02d}:{em:02d}"

def slot_duration_hours(slot_text):
    start_text, end_text = slot_text.split("-")
    return calc_effective_hours_from_time(start_text, end_text)

def slot_start_hour(slot_text):
    return int(slot_text.split("-")[0].split(":")[0])

def is_morning_slot(slot_text):
    return slot_start_hour(slot_text) < 12

def map_to_system_slot(start_time_str, end_time_str, service_text=None):
    original_slot = normalize_period_text(start_time_str, end_time_str)
    actual_hours = None
    if service_text and str(service_text).strip():
        match = re.search(r"(\d+)\s*人\s*(\d+(?:\.\d+)?)\s*小時", str(service_text))
        if match:
            actual_hours = float(match.group(2))
        else:
            match = re.search(r"(\d+(?:\.\d+)?)\s*小時", str(service_text))
            if match:
                actual_hours = float(match.group(1))
    if actual_hours is None:
        actual_hours = calc_effective_hours_from_time(start_time_str, end_time_str)
    if actual_hours is None:
        raise Exception(f"無法解析服務時段: {start_time_str}-{end_time_str}")
    if original_slot in STANDARD_SLOTS:
        return {
            "original_slot": original_slot,
            "system_slot": original_slot,
            "need_note": False,
            "sms_time": "",
            "customer_time_note": "",
        }
    sh, sm, eh, em = parse_time_slot(start_time_str, end_time_str)
    original_is_morning = sh < 12
    matched_slot = None
    for slot in STANDARD_SLOTS:
        if is_morning_slot(slot) == original_is_morning and abs(slot_duration_hours(slot) - actual_hours) < 1e-9:
            matched_slot = slot
            break
    if not matched_slot:
        raise Exception(f"找不到可對應的系統時段：原始時段 {original_slot}，時數 {actual_hours}")
    return {
        "original_slot": original_slot,
        "system_slot": matched_slot,
        "need_note": True,
        "sms_time": original_slot,
        "customer_time_note": f"服務時間：{original_slot}",
    }

def parse_service_human_hour(service_text, start_time, end_time):
    if service_text and str(service_text).strip():
        text = str(service_text).strip()
        people_match = re.search(r"(\d+)\s*人", text)
        hour_match = re.search(r"(\d+(?:\.\d+)?)\s*小時", text)
        people = int(people_match.group(1)) if people_match else 2
        if hour_match:
            hours = float(hour_match.group(1))
            return people, int(hours) if float(hours).is_integer() else hours
    hours = calc_effective_hours_from_time(start_time, end_time)
    if hours is None:
        return None, None
    return 2, int(hours) if float(hours).is_integer() else hours

def normalize_hours_text(cell_value, start_time_str=None, end_time_str=None):
    people, hours = parse_service_human_hour(cell_value, start_time_str, end_time_str)
    if hours is None:
        return f"{people}人"
    htxt = f"{int(hours)}小時" if float(hours).is_integer() else f"{hours}小時"
    return f"{people}人{htxt}"

def calc_occurrence_price(date_value, people, hours):
    return int(get_unit_price_by_date(date_value) * people * hours)

def same_address(a, b):
    return re.sub(r"\s+", "", str(a or "")) == re.sub(r"\s+", "", str(b or ""))

def build_group_key(row):
    normalized_human_hour = normalize_hours_text(row["服務人時"], row["開始時間"], row["結束時間"])
    return (
        str(row["姓名"]).strip(),
        normalize_phone(row["電話"]),
        str(row["地址"]).strip(),
        str(row["購買項目"]).strip(),
        normalize_period_text(row["開始時間"], row["結束時間"]),
        normalized_human_hour,
        str(row["備註"]).strip(),
    )

def get_region_by_address(address, accounts_config):
    for region, config in accounts_config.items():
        keywords = config.get("address_keywords", [])
        if keywords:
            for kw in keywords:
                if kw in address:
                    return region
        else:
            if region == "台北" and ("台北市" in address or "新北市" in address): return region
            if region == "台中" and "台中市" in address: return region
            if region == "桃園" and "桃園" in address: return region
            if region == "新竹" and ("新竹市" in address or "新竹縣" in address): return region
            if region == "高雄" and ("高雄市" in address or "台南市" in address): return region
    return None

def should_process_row(row):
    return str(row.get("狀態", "")).strip() == "未安排" and is_blank(row.get("訂單編號", ""))

def should_create_order(row):
    return str(row.get("狀態", "")).strip() == "未安排" and is_blank(row.get("訂單編號", ""))

def build_gsheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    if "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
        creds = Credentials.from_service_account_info(st.secrets["GOOGLE_SERVICE_ACCOUNT"], scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)

def load_worksheet(sheet_name):
    client = build_gsheet_client()
    sh = client.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(sheet_name)
    values = ws.get_all_values()
    if not values:
        raise Exception(f"工作表 {sheet_name} 沒有資料")
    headers = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)
    df["__sheet_row__"] = range(2, len(df) + 2)
    return ws, df

def ensure_columns_in_sheet(ws):
    headers = ws.row_values(1)
    required = ["簡訊實際服務時間","客人備註","訂單編號","結果","原因","沒班表日期","餘額不足未送","確認信","日曆改色結果","日曆改色原因","日曆原色","日曆新色","狀態"]
    changed = False
    for col in required:
        if col not in headers:
            headers.append(col); changed = True
    if changed:
        ws.resize(rows=max(ws.row_count, 1), cols=len(headers))
        ws.update("A1", [headers])
    return headers

def update_sheet_rows(ws, row_results):
    headers = ensure_columns_in_sheet(ws)
    header_index = {h: i + 1 for i, h in enumerate(headers)}
    updates = []
    for row_num, info in row_results.items():
        for key, value in info.items():
            if key in header_index:
                updates.append({"range": gspread.utils.rowcol_to_a1(row_num, header_index[key]), "values": [[value]]})
    if updates:
        ws.batch_update(updates)

def login(session, email, password):
    resp = session.get(LOGIN_URL, headers=HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        return False
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "_token"})
    if not token_input:
        return False
    token = token_input.get("value", "").strip()
    if not token:
        return False
    resp = session.post(LOGIN_URL, data={"_token": token, "email": email, "password": password}, headers=HEADERS, allow_redirects=True)
    return resp.status_code == 200 and "login" not in resp.url.lower()

def get_csrf_token(session):
    resp = session.get(BOOKING_URL, headers=HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        raise Exception(f"取得儲值金訂單頁失敗: {resp.status_code}")
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "_token"})
    if not token_input:
        raise Exception("無法從儲值金訂單頁提取 _token")
    token = token_input.get("value", "").strip()
    if not token:
        raise Exception("_token 為空")
    return token

def get_member(session, phone, token, clean_type_id):
    resp = session.post(GET_MEMBER_URL, data={"phone": phone, "_token": token, "clean_type_id": clean_type_id}, headers=HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        return None
    try:
        result = resp.json()
    except Exception:
        return None
    return result if isinstance(result, dict) and result.get("return_code") == "0000" and result.get("member") else None

def pick_best_address_info(member_payload, target_address):
    member = member_payload.get("member", {}) if isinstance(member_payload, dict) else {}
    purchase = member_payload.get("purchase", {}) if isinstance(member_payload, dict) else {}
    address_list = member_payload.get("address", []) if isinstance(member_payload, dict) else []
    member_address_list = member.get("memberAddressList", []) if isinstance(member, dict) else []

    for item in member_address_list:
        if same_address(item.get("address"), target_address):
            result = {
                "addressId": item.get("id", ""),
                "country_id": item.get("countryId", ""),
                "area_id": item.get("areaId", ""),
                "address": item.get("address", target_address),
                "lat": item.get("lat", ""),
                "lng": item.get("lng", ""),
                "company_id": item.get("companyId", 1),
            }
            if isinstance(item.get("purchase"), dict):
                result["purchase"] = item["purchase"]
            return result

    for item in address_list:
        if same_address(item.get("address"), target_address):
            return {
                "addressId": "",
                "country_id": item.get("countryId", ""),
                "area_id": item.get("areaId", ""),
                "address": item.get("address", target_address),
                "lat": "",
                "lng": "",
                "company_id": 1,
                "purchase": purchase if isinstance(purchase, dict) else {},
            }

    candidate_texts = []
    for item in member_address_list:
        addr = str(item.get("address", "")).strip()
        if addr:
            candidate_texts.append(addr)
    for item in address_list:
        addr = str(item.get("address", "")).strip()
        if addr:
            candidate_texts.append(addr)
    candidate_texts = list(dict.fromkeys(candidate_texts))

    if candidate_texts:
        raise Exception("找不到完全相同地址。請確認地址是否一致。候選地址：" + "；".join(candidate_texts[:8]))

    if isinstance(purchase, dict) and purchase:
        purchase_addr = str(purchase.get("address", "")).strip()
        if purchase_addr and same_address(purchase_addr, target_address):
            return {
                "addressId": "",
                "country_id": purchase.get("country_id", ""),
                "area_id": purchase.get("area_id", ""),
                "address": purchase_addr,
                "lat": purchase.get("lat", ""),
                "lng": purchase.get("lng", ""),
                "company_id": purchase.get("company_id", 1),
                "purchase": purchase,
            }
    return {}

def check_contain(session, member_id, address, lat, lng, token, clean_type_id):
    resp = session.post(CHECK_CONTAIN_URL, data={"memberId": member_id, "cleanTypeId": clean_type_id, "address": address, "lat": lat or "", "lng": lng or "", "_token": token}, headers=HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None

def calculate_hour(session, order_data, token):
    data = order_data.copy(); data["_token"] = token
    resp = session.post(CALCULATE_HOUR_URL, data=data, headers=HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None

def get_section_raw(session, order_data, token, date_slot):
    data = order_data.copy(); data["_token"] = token; data["date_list[]"] = date_slot
    resp = session.post(GET_SECTION_URL, data=data, headers=HEADERS, allow_redirects=True)
    return resp.text if resp.status_code == 200 else ""

def slot_exists_in_section_response(raw_text, date_slot):
    if not raw_text:
        return False
    date_part, period_part = date_slot.split("_", 1)
    normalized = re.sub(r"\s+", "", raw_text)
    pattern = re.escape(date_part) + r".*?" + re.escape(period_part)
    return re.search(pattern, normalized) is not None

def validate_available_slots(session, order_data, token, date_slots):
    valid_slots, invalid_slots = [], []
    for slot in date_slots:
        raw = get_section_raw(session, order_data, token, slot)
        (valid_slots if slot_exists_in_section_response(raw, slot) else invalid_slots).append(slot)
    return valid_slots, invalid_slots

def extract_order_cards_from_purchase_html(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    blocks, current = [], None
    for line in lines:
        if re.fullmatch(ORDER_NO_REGEX, line):
            if current: blocks.append(current)
            current = {"order_no": line, "lines": [line]}
        elif current:
            current["lines"].append(line)
    if current: blocks.append(current)
    return blocks

def match_order_from_purchase_page(html, target_date, target_period):
    for block in extract_order_cards_from_purchase_html(html):
        joined = "\n".join(block["lines"])
        if target_date in joined and target_period in joined:
            return block["order_no"]
    return None

def fetch_order_no_by_date_and_period(session, target_date, target_period):
    resp = session.get(PURCHASE_URL, headers=HEADERS, allow_redirects=True)
    return None if resp.status_code != 200 else match_order_from_purchase_page(resp.text, target_date, target_period)

def send_confirmation_mail(session, order_no):
    url = MAIL_SUCCESS_URL.format(order_no=order_no)
    resp = session.get(url, headers=MAIL_HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}"
    try:
        return True, str(resp.json())
    except Exception:
        return True, resp.text[:200]

def build_gcal_service():
    if not ENABLE_GCAL_COLOR_SYNC:
        return None
    scopes = ["https://www.googleapis.com/auth/calendar"]
    if "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
        credentials = Credentials.from_service_account_info(st.secrets["GOOGLE_SERVICE_ACCOUNT"], scopes=scopes)
    else:
        credentials = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scopes)
    return build("calendar", "v3", credentials=credentials)

def parse_event_time(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d")
        except Exception:
            return None

def color_name_from_id(color_id):
    mapping = {"1": "薰衣草紫","2": "鼠尾草綠","3": "葡萄紫","4": "火鶴紅","5": "香蕉黃","6": "橘子橙","7": "孔雀藍","8": "石墨灰","9": "藍莓藍","10": "羅勒綠","11": "番茄紅"}
    return mapping.get(str(color_id), f"未知({color_id})")

def find_matching_calendar_event(service, calendar_id, address, target_date, start_time_str, end_time_str):
    target_date_obj = parse_date_value(target_date)
    sh, sm, eh, em = parse_time_slot(start_time_str, end_time_str)
    tz = timezone(timedelta(hours=8))
    day_start = datetime(target_date_obj.year, target_date_obj.month, target_date_obj.day, 0, 0, 0, tzinfo=tz)
    day_end = day_start + timedelta(days=1)
    events = service.events().list(calendarId=calendar_id, timeMin=day_start.isoformat(), timeMax=day_end.isoformat(), singleEvents=True, orderBy="startTime").execute().get("items", [])
    target_addr = re.sub(r"\s+", "", str(address or "")).strip()
    for event in events:
        start_raw = event.get("start", {}).get("dateTime") or event.get("start", {}).get("date")
        end_raw = event.get("end", {}).get("dateTime") or event.get("end", {}).get("date")
        start_dt = parse_event_time(start_raw); end_dt = parse_event_time(end_raw)
        if not start_dt or not end_dt:
            continue
        location = event.get("location", "") or ""
        description = event.get("description", "") or ""
        summary = event.get("summary", "") or ""
        text_blob = re.sub(r"\s+", "", location + " " + description + " " + summary).strip()
        if start_dt.date() == target_date_obj.date() and (start_dt.hour, start_dt.minute) == (sh, sm) and (end_dt.hour, end_dt.minute) == (eh, em) and target_addr and target_addr in text_blob:
            return event
    return None

def sync_calendar_color_for_row(service, calendar_id, address, date_value, start_time_str, end_time_str):
    if not ENABLE_GCAL_COLOR_SYNC or service is None:
        return {"日曆改色結果": "未執行","日曆改色原因": "未啟用日曆改色","日曆原色": "","日曆新色": ""}
    try:
        event = find_matching_calendar_event(service, calendar_id, address, date_value, start_time_str, end_time_str)
    except HttpError as e:
        return {"日曆改色結果": "失敗","日曆改色原因": f"Calendar API 錯誤: {e}","日曆原色": "","日曆新色": ""}
    except Exception as e:
        return {"日曆改色結果": "失敗","日曆改色原因": f"Calendar 例外: {e}","日曆原色": "","日曆新色": ""}
    if not event:
        return {"日曆改色結果": "失敗","日曆改色原因": "找不到對應日曆事件","日曆原色": "","日曆新色": ""}
    event_id = event.get("id")
    old_color = str(event.get("colorId", "")); old_color_name = color_name_from_id(old_color)
    if old_color != COLOR_PURPLE:
        return {"日曆改色結果": "未改","日曆改色原因": f"需求有異動（原色：{old_color_name}）","日曆原色": old_color_name,"日曆新色": old_color_name}
    try:
        service.events().patch(calendarId=calendar_id, eventId=event_id, body={"colorId": COLOR_YELLOW}).execute()
    except HttpError as e:
        return {"日曆改色結果": "失敗","日曆改色原因": f"改色 API 錯誤: {e}","日曆原色": old_color_name,"日曆新色": old_color_name}
    except Exception as e:
        return {"日曆改色結果": "失敗","日曆改色原因": f"改色例外: {e}","日曆原色": old_color_name,"日曆新色": old_color_name}
    return {"日曆改色結果": "成功","日曆改色原因": "葡萄紫 → 香蕉黃","日曆原色": old_color_name,"日曆新色": color_name_from_id(COLOR_YELLOW)}

def prepare_base_order_data(row, member_payload, address_info, clean_type_id, people, hours, system_period, note_info):
    member = member_payload.get("member", {}) if isinstance(member_payload, dict) else {}
    last_purchase = member_payload.get("lastPurchase", {}) if isinstance(member_payload, dict) else {}
    old_purchase = address_info.get("purchase", {}) if isinstance(address_info, dict) else {}
    def pick(key, default=""):
        if old_purchase.get(key) not in (None, ""):
            return old_purchase.get(key)
        if last_purchase.get(key) not in (None, ""):
            return last_purchase.get(key)
        return default
    base_memo = ""
    if note_info["need_note"]:
        base_memo = note_info["customer_time_note"] if not base_memo else f"{base_memo}；{note_info['customer_time_note']}"
    return {
        "clean_type_id": clean_type_id,
        "phone": normalize_phone(row["電話"]),
        "name": str(member.get("name") or row["姓名"]).strip(),
        "email": str(member.get("email") or "").strip(),
        "tel": str(member.get("tel") or normalize_phone(row["電話"])),
        "line": str(member.get("line") or ""),
        "fbName": str(member.get("fb_name") or ""),
        "fb": str(member.get("fb") or ""),
        "memoProcess": str(member.get("memo_process") or ""),
        "memoFinance": str(member.get("memo_finance") or ""),
        "addressId": str(address_info.get("addressId") or ""),
        "country_id": str(address_info.get("country_id") or pick("country_id", "12")),
        "address": str(row["地址"]).strip(),
        "ping": str(pick("ping", "4")),
        "room": str(pick("room", "0")),
        "bathroom": str(pick("bathroom", "0")),
        "balcony": str(pick("balcony", "0")),
        "livingroom": str(pick("livingroom", "0")),
        "kitchen": str(pick("kitchen", "0")),
        "window": str(pick("window", "")),
        "shutter": str(pick("shutter", "")),
        "clothes": str(pick("clothes", "0")),
        "dyson": str(pick("dyson", "0")),
        "refrigerator": str(pick("refrigerator", "0")),
        "disinfection": str(pick("disinfection", "0")),
        "go_abord": str(pick("go_abord", "0")),
        "home_move": str(pick("home_move", "0")),
        "storage": str(pick("storage", "0")),
        "cabinet": str(pick("cabinet", "0")),
        "quintuple": str(pick("quintuple", "0")),
        "hour": str(int(float(hours))),
        "price": "0",
        "price_vvip": "0",
        "person": str(int(people)),
        "date_s": "",
        "period_s": system_period,
        "period": note_info["sms_time"] if note_info["need_note"] else "",
        "cycle": "1",
        "fare": "0",
        "memo": base_memo,
        "notice": "",
        "discount_code": "",
        "payway": "4",
        "is_backend": "477",
        "member_id": str(member.get("member_id") or ""),
        "company_id": str(address_info.get("company_id") or pick("company_id", "1")),
        "area_id": str(address_info.get("area_id") or pick("area_id", "25")),
        "lat": str(address_info.get("lat") or pick("lat", "")),
        "lng": str(address_info.get("lng") or pick("lng", "")),
    }

def filter_dates_by_balance(date_slots, date_prices, stored_value):
    selected_slots, selected_prices, total = [], [], 0
    for slot, price in zip(date_slots, date_prices):
        if total + price <= stored_value:
            selected_slots.append(slot); selected_prices.append(price); total += price
    return selected_slots, selected_prices, total

def stage_send_confirmation(order_no, session):
    if not order_no:
        return {"確認信": ""}
    try:
        ok, mail_msg = send_confirmation_mail(session, order_no)
        return {"確認信": "已發送" if ok else f"發送失敗: {mail_msg}"}
    except Exception as e:
        return {"確認信": f"發送失敗: {e}"}

def stage_calendar_color(row, gcal_service, region):
    calendar_id = GOOGLE_CALENDAR_MAP.get(region)
    if not calendar_id:
        return {"日曆改色結果": "未執行","日曆改色原因": f"找不到區域 {region} 的日曆設定","日曆原色": "","日曆新色": ""}
    try:
        return sync_calendar_color_for_row(gcal_service, calendar_id, str(row["地址"]).strip(), row["日期"], str(row["開始時間"]).strip(), str(row["結束時間"]).strip())
    except Exception as e:
        return {"日曆改色結果": "失敗","日曆改色原因": str(e),"日曆原色": "","日曆新色": ""}

def stage_update_status(order_no, calendar_info):
    return {"狀態": "已安排"} if order_no and calendar_info.get("日曆改色結果") == "成功" else {}

def has_action(selected_actions, action_name):
    return True if not selected_actions else action_name in selected_actions

def process_existing_order_only(row, gcal_service, region, session, selected_actions=None):
    order_no = str(row.get("訂單編號", "")).strip()
    result = {"結果": "跳過","原因": "","沒班表日期": "","餘額不足未送": "","確認信": "","日曆改色結果": "","日曆改色原因": "","日曆原色": "","日曆新色": ""}
    if not order_no:
        result["結果"] = "失敗"; result["原因"] = "無訂單編號"; return result
    did_anything = False
    if has_action(selected_actions, "寄確認信"):
        result.update(stage_send_confirmation(order_no, session)); did_anything = True
    if has_action(selected_actions, "改 Google 日曆"):
        calendar_info = stage_calendar_color(row, gcal_service, region)
        result.update(calendar_info); did_anything = True; result.update(stage_update_status(order_no, calendar_info))
    if did_anything:
        result["結果"] = "成功"
    return result

def process_one_group

def run_process_web(env_name, region, backend_email, backend_password, sheet_name, start_row, end_row, selected_actions=None, logger=print):
    global BASE_URL, ORDER_PREFIX
    if env_name == "dev":
        BASE_URL = BASE_URL_DEV; ORDER_PREFIX = ORDER_PREFIX_DEV
    else:
        BASE_URL = BASE_URL_PROD; ORDER_PREFIX = ORDER_PREFIX_PROD
    global LOGIN_URL, BOOKING_URL, PURCHASE_URL, GET_MEMBER_URL, CHECK_CONTAIN_URL, CALCULATE_HOUR_URL, GET_SECTION_URL, MAIL_SUCCESS_URL
    LOGIN_URL = f"{BASE_URL}/login"; BOOKING_URL = f"{BASE_URL}/booking/stored_value_routine"; PURCHASE_URL = f"{BASE_URL}/purchase"
    GET_MEMBER_URL = f"{BASE_URL}/ajax/get_member"; CHECK_CONTAIN_URL = f"{BASE_URL}/ajax/check_contain"; CALCULATE_HOUR_URL = f"{BASE_URL}/ajax/calculate_hour"; GET_SECTION_URL = f"{BASE_URL}/ajax/get_section"; MAIL_SUCCESS_URL = f"{BASE_URL}/purchase/mail_success/{{order_no}}"
    logger(f"目前環境：{env_name}"); logger(f"BASE_URL：{BASE_URL}"); logger(f"執行區域：{region}"); logger(f"執行工作表：{sheet_name}"); logger(f"執行列範圍：{start_row} ~ {end_row}")
    if selected_actions is None:
        selected_actions = ["建單", "寄確認信", "改 Google 日曆"]
    ws, df = load_worksheet(sheet_name)
    required_cols = ["服務人時","備註","姓名","電話","地址","日期","開始時間","結束時間","狀態","購買項目","訂單編號"]
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"工作表缺少必要欄位: {col}")
    df = df[(df["__sheet_row__"] >= start_row) & (df["__sheet_row__"] <= end_row)]
    df = df[df.apply(should_process_row, axis=1)]
    if df.empty:
        logger("沒有符合條件的資料可執行。")
        return {"success": True, "message": "沒有符合條件的資料", "failed_records": []}
    filtered_rows = [row for _, row in df.iterrows() if get_region_by_address(str(row["地址"]), ACCOUNTS) == region]
    if not filtered_rows:
        logger(f"沒有 {region} 區域的資料可執行。")
        return {"success": True, "message": f"沒有 {region} 區域資料", "failed_records": []}
    df = pd.DataFrame(filtered_rows)
    if "__sheet_row__" not in df.columns:
        raise Exception("資料缺少 __sheet_row__")
    gcal_service = None
    if ENABLE_GCAL_COLOR_SYNC:
        try:
            gcal_service = build_gcal_service(); logger("Google Calendar 已啟用")
        except Exception as e:
            logger(f"Google Calendar 初始化失敗：{e}"); gcal_service = None
    session = requests.Session()
    if not login(session, backend_email, backend_password):
        raise Exception("後台登入失敗，請確認帳號密碼")
    grouped_orders = defaultdict(list); existing_order_rows = []
    for _, row in df.iterrows():
        row_num = int(row["__sheet_row__"])
        if not has_action(selected_actions, "建單") or not should_create_order(row):
            existing_order_rows.append((row_num, row)); continue
        grouped_orders[build_group_key(row)].append((row_num, row))
    all_row_results = {}
    failed_records = []
    for row_num, row in existing_order_rows:
        try:
            result = process_existing_order_only(row, gcal_service, region, session, selected_actions)
            all_row_results[row_num] = result
            if result.get("結果") == "失敗":
                failed_records.append({"row": row_num, "name": str(row.get("姓名", "未知")).strip(), "error": str(result.get("原因", ""))})
        except Exception as e:
            all_row_results[row_num] = {"結果": "失敗", "原因": f"補處理失敗: {e}"}
            failed_records.append({"row": row_num, "name": str(row.get("姓名", "未知")).strip(), "error": f"補處理失敗: {e}"})
    for group_no, (_, rows_with_idx) in enumerate(grouped_orders.items(), start=1):
        _, first_row = rows_with_idx[0]
        logger(f"處理第 {group_no} 組：{first_row['姓名']}，共 {len(rows_with_idx)} 筆")
        try:
            token = get_csrf_token(session)
            row_results = process_one_group(session, rows_with_idx, token, gcal_service, region, None, selected_actions)
            all_row_results.update(row_results)
            for row_num, row in rows_with_idx:
                result = row_results.get(row_num, {})
                if result.get("結果") == "失敗":
                    failed_records.append({"row": row_num, "name": str(row.get("姓名", "未知")).strip(), "error": str(result.get("原因", ""))})
        except Exception as e:
            logger(f"整組失敗：{e}")
            for row_num, row in rows_with_idx:
                failed_records.append({"row": row_num, "name": str(row.get("姓名", "未知")).strip(), "error": str(e)})
                all_row_results[row_num] = {"訂單編號": "","結果": "失敗","原因": str(e),"沒班表日期": "","餘額不足未送": "","確認信": "","日曆改色結果": "","日曆改色原因": "","日曆原色": "","日曆新色": ""}
        time.sleep(REQUEST_DELAY)
    update_sheet_rows(ws, all_row_results)
    logger("已回填 Google Sheet。")
    success_count = sum(1 for v in all_row_results.values() if v.get("結果") == "成功")
    fail_count = sum(1 for v in all_row_results.values() if v.get("結果") == "失敗")
    return {"success": True, "sheet_name": sheet_name, "region": region, "env": env_name, "success_count": success_count, "fail_count": fail_count, "total_processed": len(all_row_results), "failed_records": failed_records}
