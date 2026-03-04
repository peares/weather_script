"""
ดึงข้อมูล PM2.5 + สภาพอากาศ 10 สถานี นนทบุรี-ปทุมธานี
รัน loop ทุก 1 ชั่วโมง เปิด terminal ทิ้งไว้ได้เลย
"""

import requests
import pandas as pd
from datetime import datetime
import os
import time

# ==================== CONFIG ====================
OWM_API_KEY = "ปปปปปปปปป"
LAT = 13.95
LON = 100.53
STATIONS = ['22t', 'bkp120t', 'bkp74t', 'bkp97t', '13t', 'bkp98t', 'bkp130t', '20t', 'bkp73t', 'bkp75t']
OUTPUT_FILE = "/mnt/c/Users/ak9/Documents/research-q1/nonthaburi_pm25_data.csv"
INTERVAL_HOURS = 1
MAX_RECORDS = 168  # 7 วัน
# ================================================


def fetch_air4thai(station_id):
    url = f"http://air4thai.pcd.go.th/services/getNewAQI_JSON.php?stationID={station_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        aqi_data = data.get("AQILast", {})
        pm25_info = aqi_data.get("PM25", {})
        pm25_value = pm25_info.get("value", "-1")

        try:
            pm25_float = float(pm25_value)
            if pm25_float < 0:
                pm25_float = None
        except (ValueError, TypeError):
            pm25_float = None

        return {
            "station_id": station_id,
            "station_name": data.get("nameEN", station_id),
            "pm25": pm25_float,
        }
    except Exception as e:
        print(f"  [air4thai ERROR] {station_id}: {e}")
        return None


def fetch_openweathermap(lat, lon, api_key):
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"].get("deg", 0),
        }
    except Exception as e:
        print(f"  [OpenWeatherMap ERROR] {e}")
        return None


def fetch_and_save(count, total):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{count}/{total}] {now}")

    # ดึง weather ครั้งเดียว ใช้ร่วมกันทุกสถานี
    weather_data = fetch_openweathermap(LAT, LON, OWM_API_KEY)
    if not weather_data:
        print("  ดึง weather ไม่สำเร็จ ข้ามรอบนี้")
        return False

    records = []
    for station_id in STATIONS:
        air_data = fetch_air4thai(station_id)
        if not air_data:
            continue

        record = {
            "timestamp": now,
            "station_id": air_data["station_id"],
            "station_name": air_data["station_name"],
            "pm25": air_data["pm25"],
            "wind_speed": weather_data["wind_speed"],
            "wind_direction": weather_data["wind_direction"],
            "temperature": weather_data["temperature"],
            "humidity": weather_data["humidity"],
            "pressure": weather_data["pressure"],
        }
        records.append(record)
        print(f"  {station_id}: pm25={record['pm25']}")

    if records:
        df_new = pd.DataFrame(records)
        if os.path.exists(OUTPUT_FILE):
            df_new.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
        else:
            df_new.to_csv(OUTPUT_FILE, index=False)
        print(f"  บันทึก {len(records)} สถานี → {OUTPUT_FILE}")

    return True


def main():
    print("=" * 55)
    print(f"เริ่มเก็บข้อมูล {len(STATIONS)} สถานี ทุก {INTERVAL_HOURS} ชั่วโมง")
    print(f"รวม {MAX_RECORDS} รอบ = 7 วัน")
    print("กด Ctrl+C เพื่อหยุด")
    print("=" * 55)

    for i in range(1, MAX_RECORDS + 1):
        fetch_and_save(i, MAX_RECORDS)
        if i < MAX_RECORDS:
            print(f"  รอ {INTERVAL_HOURS} ชั่วโมง...")
            time.sleep(INTERVAL_HOURS * 3600)

    print("\nเสร็จสิ้น! เก็บข้อมูลครบ 7 วันแล้วครับ")


if __name__ == "__main__":
    main()
