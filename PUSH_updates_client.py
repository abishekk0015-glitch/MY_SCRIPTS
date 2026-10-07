import csv
import json
from datetime import datetime
import requests

# Configuration
CSV_FILE = "Push.csv"
UPDATE_STATUS_URL = "http://10.24.32.149:80/shipments/updateStatus"

HEADERS = {
    "X_TOPIC_NAME": "e2e.ns.externalization.prod",
    "X_SHIPMENT_SIZE": "LARGE",
    "X_EVENT_SOURCE": "GSM",
    "X_EVENT_NAME": "shipment_delivered",
    "Content-Type": "application/json",
}

def process_csv():
    try:
        with open(CSV_FILE, mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            tracking_ids = [row[0].strip() for row in reader if row and row[0].strip()]

            print(f"--- Starting Processing of {CSV_FILE} ---")
            print(f"Total Tracking IDs found in file: {len(tracking_ids)}")

            for count, entity_id in enumerate(tracking_ids, start=1):
                print(f"\n[{count}] Processing Tracking ID: {entity_id}...")

                formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                payload = {
                    "vendor_tracking_id": entity_id,
                    "merchant_reference_id": entity_id,
                    "merchant_code": "NAH",
                    "merchant_name": "NAAPTOL",
                    "shipment_type": "OUTGOING",
                    "location": "Satellitehub_PCMC",
                    "status": "delivered",
                    "event": "shipment_delivered",
                    "event_date": formatted_date,
                    "reason": "",
                    "sub_reasons": [None],
                }

                # PRINT PAYLOAD
                print("Sending Payload:")
                print(json.dumps(payload, indent=4))

                # Send Request
                try:
                    response = requests.post(
                        UPDATE_STATUS_URL,
                        headers=HEADERS,
                        json=payload,
                        timeout=10
                    )

                    if response.status_code == 200:
                        print(f"Successfully processed. Response: {response.text}")
                    else:
                        print(f"Failed. Status Code: {response.status_code}")
                        print(f"Response: {response.text}")
                except Exception as e:
                    print(f"Network or request error during updateStatus: {e}")

                print("-" * 50)

    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found.")

if __name__ == "__main__":
    process_csv()
