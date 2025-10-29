
import json, pandas as pd
import os, requests, json, time
from pathlib import Path
import numpy as np

import os, json, time
import pandas as pd
import numpy as np
from dateutil import parser as dtparser
from tqdm import tqdm
import paho.mqtt.client as mqtt

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

DATA_FOLDER = "data_raw"

# 1. Task1
'''
Task 1: Data Retrieval		
Purpose: Pull power + emission data for the Eraring facility from the OpenElectricity API.
Output: Two JSON files (*_power_5m.json, *_emissions_5m.json)
'''

def task_1_data_retrieval ():
    print("task 1 START")
    # 1.0 request avaliability checking
    '''
    API_KEY = "oe_3ZkPHh9ctU2nmLzHQhVi26tZ"
    r = requests.get(
        "https://api.openelectricity.org.au/v4/me",
        headers={"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"},
        timeout=20
    )
    print("status:", r.status_code)
    print(r.text)
    r.raise_for_status()
    '''
    
    # 1.1 retrieval configs
    API_BASE = "https://api.openelectricity.org.au/v4"
    API_KEY = "oe_3ZkPHh9ctU2nmLzHQhVi26tZ"
    HEADERS = {"Authorization": f"Bearer {API_KEY}"}

    url = f"{API_BASE}/data/facilities/NEM"
    common = {
        "interval": "5m",
        "facility_code": ["ERARING"],              
        "date_start": "2025-10-06T00:00:00",     
        "date_end":   "2025-10-12T23:55:00",
    }
    Path("data_raw").mkdir(exist_ok=True)

    # 1.2 requests:
    # power
    p = requests.get(url, headers=HEADERS, params={**common, "metrics": ["power"]}, timeout=60)
    p.raise_for_status()
    with open("data_raw/NEM_ERARING_power_5m.json", "w", encoding="utf-8") as f:
        json.dump(p.json(), f, ensure_ascii=False, indent=2)
    print("1.1 retrieved power saved")

    time.sleep(0.5)  

    # emissions
    e = requests.get(url, headers=HEADERS, params={**common, "metrics": ["emissions"]}, timeout=60)
    e.raise_for_status()
    with open("data_raw/NEM_ERARING_emissions_5m.json", "w", encoding="utf-8") as f:
        json.dump(e.json(), f, ensure_ascii=False, indent=2)
    print("1.1 retrieved emissions saved")

    print("task 1 retrieval DONE")
    print("")
    return

#################################################################################################
# Task2
'''
Task 2: Data Integration & Cleaning
Purpose: Convert both JSONs → CSV, merge them, clean values, and compute emission intensity.
Output: NEM_ERARING_power_emissions_cleaned.csv
'''
def task_2_data_integration_and_cleaning():
    print("task 2 START")
    # 2.1 merge power.csv
    with open("data_raw/NEM_ERARING_power_5m.json", encoding="utf-8") as f:
        j_power = json.load(f)

    rows = []
    for block in j_power.get("data", []):
        if block.get("metric") != "power":
            continue
        for series in block.get("results", []):
            unit_code = series.get("columns", {}).get("unit_code") or series.get("name")
            for ts, val in series.get("data", []):
                rows.append({"timestamp": ts, "unit_code": unit_code, "power_MW": val})
    df_power = pd.DataFrame(rows)
    df_power.to_csv("data_raw/NEM_ERARING_power_5m.csv", index=False)
    print("2.1 merge result saved: data_raw/NEM_ERARING_power_5m.csv")

    # 2.2 merge emissions.csv
    with open("data_raw/NEM_ERARING_emissions_5m.json", encoding="utf-8") as f:
        j_emis = json.load(f)

    rows = []
    for block in j_emis.get("data", []):
        if block.get("metric") != "emissions":
            continue
        for series in block.get("results", []):
            unit_code = series.get("columns", {}).get("unit_code") or series.get("name")
            for ts, val in series.get("data", []):
                rows.append({"timestamp": ts, "unit_code": unit_code, "emissions_tCO2e": val})
    df_emis = pd.DataFrame(rows)
    df_emis.to_csv("data_raw/NEM_ERARING_emissions_5m.csv", index=False)
    print("2.1 merge result saved: data_raw/NEM_ERARING_emissions_5m.csv")

    # 2.3 data cleaning steps
    # 1. read files
    df_p = pd.read_csv("data_raw/NEM_ERARING_power_5m.csv")
    df_e = pd.read_csv("data_raw/NEM_ERARING_emissions_5m.csv")
    # 2. exchange time
    df_p["timestamp"] = pd.to_datetime(df_p["timestamp"])
    df_e["timestamp"] = pd.to_datetime(df_e["timestamp"])
    # 3. merge
    df = pd.merge(df_p, df_e, on=["timestamp", "unit_code"], how="outer").sort_values("timestamp")
    # 4. data clean
    df["power_MW"] = pd.to_numeric(df["power_MW"], errors="coerce")
    df["emissions_tCO2e"] = pd.to_numeric(df["emissions_tCO2e"], errors="coerce") #type transfer
    df = df.drop_duplicates(subset=["timestamp", "unit_code"]) #delete multiply
    df = df[(df["power_MW"] >= 0) & (df["emissions_tCO2e"] >= 0)] #delete outlier
    df[["power_MW", "emissions_tCO2e"]] = df[["power_MW", "emissions_tCO2e"]].replace(0, np.nan)
    # 5. emission intensity
    df["emission_intensity_tCO2_MWh"] = df["emissions_tCO2e"] / df["power_MW"]
    # 6. output
    df.to_csv("data_raw/NEM_ERARING_power_emissions_cleaned.csv", index=False)
    print("2.2 cleaning result saved → data_raw/NEM_ERARING_power_emissions_cleaned.csv")
    
    print("task 2 merge & cleaning DONE")
    print("")
    return
################################################################
# Task3
'''
Task3: MQTT Data Publisher
Purpose: 
- Reads from your cleaned CSV file.
- Converts each row into a JSON message.
- Sends (publishes) it to a topic
Output: Live messages, on TOPIC = nem/usyd/comp5339/540124550/stream
'''

# 3.0 MQTT consifs
# basic settings
CSV_PATH   = "data_raw/NEM_ERARING_power_emissions_cleaned.csv"
BROKER     = "test.mosquitto.org"     
PORT       = 1883
TOPIC      = "nem/usyd/comp5339/540124550/stream"  
SLEEP_SEC  = 0.10                      
QOS        = 0
RETAIN     = False

# Connection handlers
def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[MQTT] on_connect: {reason_code}")

def on_disconnect(client, userdata, reason_code, properties=None):
    print(f"[MQTT] on_disconnect: {reason_code}")

# Timestamp conversion helper: whatever timestamp in .csv -> ISO 8601 str
def coerce_ts_to_iso8601(x):
    if pd.isna(x): return None
    try:
        return dtparser.parse(str(x)).isoformat()
    except Exception:
        return None

# Build one .json message
def row_to_payload(row, have_cols):
    get = row.get
    # Takes one row of the .csv and converts it into .json dict acceptable format.
    # key fields: always include.
    payload = {
        "network": "NEM",
        "facility_id": get("unit_code"),
        "timestamp": coerce_ts_to_iso8601(get("timestamp")),
        "power_MW": float(get("power_MW")) if pd.notna(get("power_MW")) else None,
        "emissions_tCO2e": float(get("emissions_tCO2e")) if pd.notna(get("emissions_tCO2e")) else None,
    }

    # extra metadata
    if "emission_intensity_tCO2_MWh" in have_cols:
        v = get("emission_intensity_tCO2_MWh")
        payload["emission_intensity_tCO2_MWh"] = float(v) if pd.notna(v) else None
    if "facility_name" in have_cols:
        payload["facility_name"] = get("facility_name")
    if "region" in have_cols:
        payload["region"] = get("region")
    if "fuel_tech" in have_cols:
        payload["fuel_tech"] = get("fuel_tech")

    # Attaches location -> nested dictionary when latitude/longitude are present.
    # then each row becomes a compact JSON “event” -> ready to publish.
    lat = get("lat") if "lat" in have_cols else None
    lon = get("lon") if "lon" in have_cols else None
    if pd.notna(lat) and pd.notna(lon):
        payload["location"] = {"lat": float(lat), "lon": float(lon)}
    else:
        payload["location"] = None
    return payload

# def one_complete_MQTT_round():
def task_3_MQTT_data_publishing():
    print("task 3 START")
    # 1. access data source
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    df = pd.read_csv(CSV_PATH)
    have_cols = set(df.columns)

    for c in ["timestamp", "unit_code", "power_MW", "emissions_tCO2e"]:
        if c not in have_cols:
            raise ValueError(f"Missing required column: {c}")

    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.assign(_ts=ts).sort_values(["_ts", "unit_code"]).drop(columns=["_ts"]).reset_index(drop=True)

    # 2. Create MQTT client and hooks up the callbacks:
    client = mqtt.Client(
        client_id="publisher-540124550",
        protocol=mqtt.MQTTv5,
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # 3. Publisher Initialization
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    # 4. run the publishing loop
    try:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Publishing"):
            row = row.to_dict()
            payload = row_to_payload(row, have_cols)
            if not payload["timestamp"] or not payload["facility_id"]:
                continue
            client.publish(TOPIC, json.dumps(payload), qos=QOS, retain=RETAIN)
            time.sleep(SLEEP_SEC)
    finally:
        # MQTT should always be cleanly stopped and disconnected
        client.loop_stop()
        client.disconnect()
        print("task 3 MQTT publish topic DONE")

if __name__ == "__main__":
    import time
    ROUNDS = 3
    for round_i in range(1, ROUNDS + 1):
        print(f"\n[MAIN] [1_to_3 script] Round {round_i} starting...")
        task_1_data_retrieval()
        task_2_data_integration_and_cleaning()
        task_3_MQTT_data_publishing()
        if round_i < ROUNDS:
            print("[MAIN] [1_to_3 script] Sleeping 60s before next round...")
            time.sleep(60)
    print("[MAIN] [1_to_3 script] Publisher finished all rounds.")



