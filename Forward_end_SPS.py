import requests
import json
import csv
import sys
from datetime import datetime

# --- Compatibility fix for Python 2 and 3 ---
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

if sys.version_info[0] < 3:
    csv_open_kwargs = {'mode': 'rb'}
else:
    csv_open_kwargs = {'mode': 'r', 'newline': '', 'encoding': 'utf-8'}

def get_current_ist_time():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+05:30')

# --- Configuration ---
csv_file_path = '/home/abisheka.vc/KrleDataFix.csv'
base_url = "http://10.24.9.223:80/abl/events"
auth_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZrLWU0NWU2MzIz..."

try:
    with open("output_successful.txt", "a") as success_file, \
         open("output_failed.txt", "a") as failed_file, \
         open(csv_file_path, **csv_open_kwargs) as file:

        csv_reader = csv.reader(file)

        for row in csv_reader:
            if not row or not row[0]:
                continue

            entity_id = row[0].strip()
            current_ts = get_current_ist_time()

            print("\n" + "="*60)
            print("PROCESSING: {}".format(entity_id))
            print("="*60)

            try:
                # 1. Fetch from Accounting API (To get the ORIGINAL activity payload)
                acc_url = "http://10.24.39.115/events/activity_type/forward_start/activity_id/{}".format(entity_id)
                acc_res = requests.get(acc_url, timeout=10)

                if acc_res.status_code != 200:
                    raise Exception("Accounting API failed: {}".format(acc_res.status_code))

                acc_json = acc_res.json()
                # The actual shipment data is usually nested inside 'commerce_activity' -> 'payload'
                raw_payload_str = acc_json.get('commerce_activity', {}).get('payload', '{}')
                source_payload = json.loads(raw_payload_str)

                # 2. Build Payload mapping values from the Accounting API response
                # We use .get() to avoid KeyErrors if a specific record is missing a field
                payload = {
                    "customer": {
                        "name": source_payload.get("customer", {}).get("name", "Valued Customer"),
                        "pincode": source_payload.get("customer", {}).get("pincode", source_payload.get("destination_pin_code", ""))
                    },
                    "merchant_code": source_payload.get("merchant_code", "EMH"),
                    "entity_id": entity_id,
                    "destination_pin_code": source_payload.get("destination_pin_code", ""),
                    "entity_type": source_payload.get("entity_type", "OutgoingShipment"),
                    "cod_value": str(source_payload.get("cod_value", "0.0")),
                    "origin_facility_pin_code": source_payload.get("origin_facility_pin_code", ""),
                    "vendor_code": source_payload.get("vendor_code", "FSD_LARGE_COD"),
                    "vendor_tracking_id": source_payload.get("vendor_tracking_id", entity_id),
                    "source": source_payload.get("source", "E2E"),
                    "request_type": source_payload.get("request_type", "large"),
                    "merchant_reference_id": source_payload.get("merchant_reference_id", ""),
                    "payment_type": source_payload.get("payment_type", "COD"),
                    "shipment": {
                        "shipment_dimension": source_payload.get("shipment", {}).get("shipment_dimension", {
                            "breadth": 0.0, "length": 0.0, "height": 0.0
                        }),
                        "shipment_items": source_payload.get("shipment", {}).get("shipment_items", []),
                        "value": float(source_payload.get("shipment", {}).get("value", 0.0)),
                        "shipment_weight": source_payload.get("shipment", {}).get("shipment_weight", {"unit": "KG", "value": 0.0}),
                        "shipment_id": source_payload.get("shipment", {}).get("shipment_id", ""),
                        "dead_weight": source_payload.get("shipment", {}).get("dead_weight", {"unit": "KG", "value": 0.0})
                    },
                    "event_date": current_ts,
                    "rto_confirmed_date": source_payload.get("rto_confirmed_date"),
                    "event": "forward_end",
                    "dispatched_date": current_ts
                }

                # 3. Build Headers
                headers = {
                    "X_TOPIC_NAME": "ekl.e.accounting.events.production",
                    "X_EVENT_SOURCE": "GSM",
                    "X_SERVICE_TYPE": "NONFA_FORWARD_LARGE_E2E_EKART",
                    "X_SHIPMENT_SIZE": source_payload.get("request_type", "LARGE").upper(),
                    "X_ER_STATUS": "NEW",
                    "X_EVENT_NAME": "forward_end",
                    "X_MERCHANT_CODE": source_payload.get("merchant_code", "EMH"),
                    "Content-Type": "application/json",
                    "Authorization-proxy": "Bearer {}".format(auth_token)
                }

                # --- Debug Print ---
                print(">>> PAYLOAD FETCHED FROM ACCOUNTING API FOR: {}".format(entity_id))

                # 4. POST Request
                resp = requests.post(base_url, headers=headers, data=json.dumps(payload), timeout=15)
                print("<<< RESPONSE: {} - {}".format(resp.status_code, resp.text))

                if resp.status_code in [200, 201]:
                    success_file.write("{}\n".format(entity_id))
                else:
                    failed_file.write("{}: {} - {}\n".format(entity_id, resp.status_code, resp.text))

            except Exception as e:
                print("!!! ERROR: {}".format(e))
                failed_file.write("{}: {}\n".format(entity_id, str(e)))

except Exception as e:
    print("FATAL ERROR: {}".format(e))
