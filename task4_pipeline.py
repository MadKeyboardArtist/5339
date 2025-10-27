# ==============================================================
# Task 4: Data Subscription & Continuous Map Visualization
# ==============================================================

import json, time, threading
import paho.mqtt.client as mqtt
import folium
from folium.plugins import MarkerCluster
from pathlib import Path

# ============ MQTT Settings ============
BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "nem/usyd/comp5339/540124550/stream"

# ============ Global Variables ============
facility_data = {}
MAP_PATH = Path("latest_map.html")

# Create one persistent map and marker cluster
m = folium.Map(location=[-33.8, 151.0], zoom_start=6, tiles="cartodb positron")
marker_cluster = MarkerCluster().add_to(m)
m.save(MAP_PATH)  # initial blank map
print(f"[MAP] Created base map at {MAP_PATH.resolve()}")

# ============ MQTT Callbacks ============
def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[SUB] Connected with result code {reason_code}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    """Handle each incoming MQTT message."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        fid = payload.get("facility_id")
        loc = payload.get("location")

        if not loc or "lat" not in loc or "lon" not in loc:
            return

        lat, lon = loc["lat"], loc["lon"]
        power = payload.get("power_MW")
        emis = payload.get("emissions_tCO2e")
        popup = f"<b>{fid}</b><br>Power: {power} MW<br>Emissions: {emis} tCO2e"

        # store/update facility state
        facility_data[fid] = dict(lat=lat, lon=lon, power=power, emis=emis)

    except Exception as e:
        print("[SUB] Message error:", e)

# ============ Map Auto-Updater Thread ============
def map_updater(interval=5):
    """Re-save the map every few seconds with current facility data."""
    while True:
        global marker_cluster
        # clear existing cluster and re-add markers
        marker_cluster = MarkerCluster().add_to(m)
        for fid, d in facility_data.items():
            popup = f"<b>{fid}</b><br>Power: {d['power']} MW<br>Emissions: {d['emis']} tCO2e"
            folium.Marker([d["lat"], d["lon"]], popup=popup).add_to(marker_cluster)

        m.save(MAP_PATH)
        print(f"[MAP] Updated: {len(facility_data)} facilities -> {MAP_PATH.name}")
        time.sleep(interval)

# ============ Run the Subscriber ============
def run_subscriber():
    client = mqtt.Client(client_id="subscriber-540124550", protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, keepalive=60)

    # start map-updater thread
    threading.Thread(target=map_updater, daemon=True).start()

    print("[SUB] Listening for messages and updating map...")
    print(f"[INFO] Open {MAP_PATH.resolve()} in your browser and refresh every few seconds.")
    client.loop_forever()

# ============ Entry Point ============
if __name__ == "__main__":
    run_subscriber()
