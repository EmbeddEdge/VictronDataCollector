import requests
from influxdb_client import InfluxDBClient, Point, WriteOptions
import time

VRM_API = "https://vrmapi.victronenergy.com/v2"
TOKEN = "YOUR_API_TOKEN"
SITE_ID = "YOUR_SITE_ID"

headers = {"X-Authorization": f"Bearer {TOKEN}"}

# InfluxDB
influx = InfluxDBClient(url="http://localhost:8086", token="INFLUX_TOKEN", org="your-org")
write_api = influx.write_api(write_options=WriteOptions(batch_size=1))

def fetch_data():
    url = f"{VRM_API}/installations/{SITE_ID}/stats"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

while True:
    data = fetch_data()
    soc = data["records"]["soc"]["y"][-1]  # last SoC value
    point = Point("battery").field("soc", soc)
    write_api.write(bucket="your-bucket", record=point)
    time.sleep(60)  # every minute