import os
import requests
import json
import csv

# Get system username cleanly across platforms
username = os.path.basename(os.getcwd())

def get_user_input():
    track_type = input("Hey {}! What is the trackType for the shipment? (e.g., CREATE_SR): ".format(username)).strip()
    sr_type = input("Hey {}! What is the srType? (e.g., LASTMILE/LASTMILE_RVP): ".format(username)).strip()
    sr_id = input("Hey {}! What is the srId?: ".format(username)).strip()
    parent_sr_id = input("Hey {}! What is the parentSrId?: ".format(username)).strip()

    vendor_input = input("Choose vendor (ekart, delhivery, ecom, shadowfax-reverse, xbees, bvc, shadowfax-cod, shadowfax): ").strip().lower()

    # Updated mapping to fix 412 "Service not onboarded" error for Shadowfax forward flows
    valid_vendors = {
        'ekart': 'flipkartlogistics',
        'delhivery': 'delhivery_consolidation',
        'ecom': 'ecom_consolidation',
        'shadowfax-reverse': 'shadowfax_reverse',
        'shadowfax': 'shadowfax',
        'shadowfax-cod': 'shadowfax',  # Standard forward flow endpoint mapping
        'xbees': 'expressbees_consolidation',
        'bvc': 'bvc_eship'
    }

    if vendor_input not in valid_vendors:
        print("Galat choice! Options in dictionary: {}".format(list(valid_vendors.keys())))
        exit(1)

    return track_type, sr_type, sr_id, parent_sr_id, valid_vendors[vendor_input]

def get_payload(track_type, sr_type, sr_id, parent_sr_id):
    shipping_lite_url = "http://10.24.1.47/srTrackStatus/getSpokesPayload?trackType={}&srType={}&srId={}&parentSrId={}".format(
        track_type, sr_type, sr_id, parent_sr_id)
    print("\nConstructed URL: {}".format(shipping_lite_url))

    response = requests.get(shipping_lite_url)

    if response.status_code == 200:
        # Parse JSON to clean up double-escaped strings
        try:
            payload_data = response.json()
            if isinstance(payload_data, str):
                payload_data = json.loads(payload_data)
        except json.JSONDecodeError:
            payload_data = response.text

        print("Mubarak ho! GET call successful.")
        return payload_data
    else:
        print("Failed GET call with status code {}.".format(response.status_code))
        exit(1)

def post_request(payload_data, vendor_code, sr_type):
    if sr_type.upper() in ['LASTMILE_RVP', 'CREATERVP']:
        spokes_url = "http://10.24.2.105/spokes/utility/transform/request/flipkart/{}/create_rvp".format(vendor_code)
    else:
        spokes_url = "http://10.24.2.105/spokes/utility/transform/request/flipkart/{}/create".format(vendor_code)

    print("POST URL: {}".format(spokes_url))

    # Send payload as clean JSON body
    if isinstance(payload_data, (dict, list)):
        response = requests.post(spokes_url, json=payload_data)
    else:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(spokes_url, headers=headers, data=payload_data)

    if response.status_code == 200:
        with open('post_response.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Response'])
            writer.writerow([response.text])

        print("Post call successful! Response saved to 'post_response.csv'.")
    else:
        print("\nPOST call failed with status code {}.".format(response.status_code))
        print("Response detail:", response.text)
        exit(1)

def main():
    track_type, sr_type, sr_id, parent_sr_id, vendor_code = get_user_input()
    payload = get_payload(track_type, sr_type, sr_id, parent_sr_id)
    post_request(payload, vendor_code, sr_type)

if __name__ == "__main__":
    main()
