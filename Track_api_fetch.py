import csv
import requests
import json
import os

INPUT_FILE = "/home/abisheka.vc/track.csv"
OUTPUT_FILE = "sr.csv"
LOG_FOLDER = "response_logs"
API_URL = "http://10.24.1.7/service-requests/internal/track?trackingId="

# Create folder to log raw responses
os.makedirs(LOG_FOLDER, exist_ok=True)

with open(INPUT_FILE, 'r') as infile, open(OUTPUT_FILE, 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # Write output CSV header
    writer.writerow(["trackingId", "serviceRequestId", "serviceCompletionDate", "status", "serviceRequestType"])

    next(reader)  # Skip header line

    for row in reader:
        if not row:
            continue

        tracking_id = row[0].strip().strip('"')

        if not tracking_id:
            continue

        print(f"🔍 Fetching for {tracking_id}...")

        try:
            response = requests.get(f"{API_URL}{tracking_id}")
            data = response.json()

            service = data.get("payload", {}).get("serviceRequest", {})

            # Convert all fields to strings safely
            srid = str(service.get("serviceRequestId", "N/A"))
            date = str(service.get("serviceCompletionDate", "N/A"))
            status = str(service.get("status", "N/A"))
            req_type = str(service.get("serviceRequestType", "N/A"))

            # Validate full SRID
            if srid == "N/A" or len(srid) < 10:
                with open(f"{LOG_FOLDER}/bad_{tracking_id}.json", "w") as logf:
                    json.dump(data, logf, indent=2)

            # Write to CSV
            writer.writerow([tracking_id, srid, date, status, req_type])

        except Exception as e:
            print(f"❌ Error for {tracking_id}: {str(e)}")
            with open(f"{LOG_FOLDER}/error_{tracking_id}.txt", "w") as logf:
                logf.write(str(e))
