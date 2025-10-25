# Task1

import json, pandas as pd
import os, requests, json, time
from pathlib import Path
import numpy as np
API_KEY = "oe_3ZkPHh9ctU2nmLzHQhVi26tZ"  

r = requests.get(
    "https://api.openelectricity.org.au/v4/me",
    headers={"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"},
    timeout=20
)
print("status:", r.status_code)
print(r.text)
r.raise_for_status()


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

# 1) power
p = requests.get(url, headers=HEADERS, params={**common, "metrics": ["power"]}, timeout=60)
p.raise_for_status()
with open("data_raw/NEM_ERARING_power_5m.json", "w", encoding="utf-8") as f:
    json.dump(p.json(), f, ensure_ascii=False, indent=2)
print("power saved")

time.sleep(0.5)  

# 2) emissions
e = requests.get(url, headers=HEADERS, params={**common, "metrics": ["emissions"]}, timeout=60)
e.raise_for_status()
with open("data_raw/NEM_ERARING_emissions_5m.json", "w", encoding="utf-8") as f:
    json.dump(e.json(), f, ensure_ascii=False, indent=2)
print("emissions saved")

#################################################################################################
# Task2

#  power.csv

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
print("saved： data_raw/NEM_ERARING_power_5m.csv")

# emissions.csv
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
print(" saved ： data_raw/NEM_ERARING_emissions_5m.csv")

# 1.read files
df_p = pd.read_csv("data_raw/NEM_ERARING_power_5m.csv")
df_e = pd.read_csv("data_raw/NEM_ERARING_emissions_5m.csv")
# 2. exchange time
df_p["timestamp"] = pd.to_datetime(df_p["timestamp"])
df_e["timestamp"] = pd.to_datetime(df_e["timestamp"])
# 3. merge
df = pd.merge(df_p, df_e, on=["timestamp", "unit_code"], how="outer").sort_values("timestamp")
# 4.data clean
df["power_MW"] = pd.to_numeric(df["power_MW"], errors="coerce")
df["emissions_tCO2e"] = pd.to_numeric(df["emissions_tCO2e"], errors="coerce") #type transfer
df = df.drop_duplicates(subset=["timestamp", "unit_code"]) #delete multiply
df = df[(df["power_MW"] >= 0) & (df["emissions_tCO2e"] >= 0)] #delete outlier
df[["power_MW", "emissions_tCO2e"]] = df[["power_MW", "emissions_tCO2e"]].replace(0, np.nan)
# 5.emission intensity
df["emission_intensity_tCO2_MWh"] = df["emissions_tCO2e"] / df["power_MW"]
# 6. 导出
df.to_csv("data_raw/NEM_ERARING_power_emissions_cleaned.csv", index=False)
print("saved → data_raw/NEM_ERARING_power_emissions_cleaned.csv")


################################################################
# Task3

import os, json, time
import pandas as pd
import numpy as np
from dateutil import parser as dtparser
from tqdm import tqdm
import paho.mqtt.client as mqtt


CSV_PATH   = "data_raw/NEM_ERARING_power_emissions_cleaned.csv"
BROKER     = "test.mosquitto.org"     
PORT       = 1883
TOPIC      = "nem/usyd/comp5339/540124550/stream"  
SLEEP_SEC  = 0.10                      
QOS        = 0
RETAIN     = False

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[MQTT] on_connect: {reason_code}")

def on_disconnect(client, userdata, reason_code, properties=None):
    print(f"[MQTT] on_disconnect: {reason_code}")

def coerce_ts_to_iso8601(x):
    if pd.isna(x): return None
    try:
        return dtparser.parse(str(x)).isoformat()
    except Exception:
        return None

def row_to_payload(row, have_cols):
    get = row.get
    payload = {
        "network": "NEM",
        "facility_id": get("unit_code"),
        "timestamp": coerce_ts_to_iso8601(get("timestamp")),
        "power_MW": float(get("power_MW")) if pd.notna(get("power_MW")) else None,
        "emissions_tCO2e": float(get("emissions_tCO2e")) if pd.notna(get("emissions_tCO2e")) else None,
    }
    if "emission_intensity_tCO2_MWh" in have_cols:
        v = get("emission_intensity_tCO2_MWh")
        payload["emission_intensity_tCO2_MWh"] = float(v) if pd.notna(v) else None
    if "facility_name" in have_cols:
        payload["facility_name"] = get("facility_name")
    if "region" in have_cols:
        payload["region"] = get("region")
    if "fuel_tech" in have_cols:
        payload["fuel_tech"] = get("fuel_tech")
    lat = get("lat") if "lat" in have_cols else None
    lon = get("lon") if "lon" in have_cols else None
    if pd.notna(lat) and pd.notna(lon):
        payload["location"] = {"lat": float(lat), "lon": float(lon)}
    else:
        payload["location"] = None
    return payload

def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    df = pd.read_csv(CSV_PATH)
    have_cols = set(df.columns)

    for c in ["timestamp", "unit_code", "power_MW", "emissions_tCO2e"]:
        if c not in have_cols:
            raise ValueError(f"Missing required column: {c}")

    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.assign(_ts=ts).sort_values(["_ts", "unit_code"]).drop(columns=["_ts"]).reset_index(drop=True)

    client = mqtt.Client(
        client_id="publisher-540124550",
        protocol=mqtt.MQTTv5,
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    try:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Publishing"):
            row = row.to_dict()
            payload = row_to_payload(row, have_cols)
            if not payload["timestamp"] or not payload["facility_id"]:
                continue
            client.publish(TOPIC, json.dumps(payload), qos=QOS, retain=RETAIN)
            time.sleep(SLEEP_SEC)
    finally:
        client.loop_stop()
        client.disconnect()
        print("Done.")

if __name__ == "__main__":
    main()

import paho.mqtt.client as mqtt

BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "nem/usyd/comp5339/540124550/stream"  

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[SUB] Connected with result code {reason_code}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    print(f"[SUB] Received from {msg.topic}:\n{msg.payload.decode('utf-8', errors='ignore')}\n")

client = mqtt.Client(client_id="subscriber-demo", protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, 60)

# (in outcome)
#client.loop_forever()

# (in testing)
client.loop_start()
time.sleep(60)
client.loop_stop()
client.disconnect()



