import os
import requests
import json
import csv
from datetime import datetime

###
 # Created by bikrant.sahoo on 02/12/23.
###

print("""

                             ,,
`7MMF'     A     `7MF'     `7MM
  `MA     ,MA     ,V         MM
   VM:   ,VVM:   ,V .gP"Ya   MM  ,p6"bo   ,pW"Wq.`7MMpMMMb.pMMMb.  .gP"Ya
    MM.  M' MM.  M',M'   Yb  MM 6M'  OO  6W'   `Wb MM    MM    MM ,M'   Yb
    `MM A'  `MM A' 8M""""""  MM 8M       8M     M8 MM    MM    MM 8M""""""
     :MM;    :MM;  YM.    ,  MM YM.    , YA.   ,A9 MM    MM    MM YM.    ,
      VF      VF    `Mbmmd'.JMML.YMbmd'   `Ybmd9'.JMML  JMML  JMML.`Mbmmd'


""")

cwd = os.getcwd()
username = cwd.split('/')[-1]
input_data = "3ps.csv"
input_csv = os.path.join(cwd, input_data)

user_input = input("\033[91mHey {}! Good day! What is the status you want to update? \033[0m ".format(username))

confirmation = input("\033[91mHey {}, are you sure? (yes/no): \033[0m".format(username))
if confirmation.lower() != 'yes':
    exit("Update aborted. It seems you have some other plans")

user_marked = input("\033[91mHey {}, please let me know the user under which it will be marked (LDAP or mail id): \033[0m ".format(username))

results = []

with open(input_csv, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        dynamic_value = row[0]
        r2d2_url = 'http://10.24.1.19/shipments/vendor_tracking_id/{}'.format(dynamic_value)

        response = requests.get(r2d2_url)
        data = response.json()
        service_request_id = data.get('service_request_id')

        topic_publish = {
            "shipments": {
                "status": user_input,
                "trackingId": dynamic_value,
                "remarks": "Marking on request/confirmation of {}".format(user_marked),
                "creationDate": datetime.now().isoformat(),
                "eventTimeStamp": datetime.now().isoformat(),
                "statusDescription": "Marking on request/confirmation of {}".format(user_marked),
                "serviceRequestId": service_request_id,
                "context": {
                    "event_tat": datetime.now().isoformat()
                }
            },
            "parentSrId": service_request_id,
            "clientId": "oncall"
        }

        lite_url = 'http://10.24.1.47/shipments/e2e/updateTrackStatus?requestType=TRACKE2E'
        headers = {'Content-Type': 'application/json', 'X_CLIENT_ID': 'oncall'}
        response_second_api = requests.post(lite_url, headers=headers, json=topic_publish)

        result = {
            "trackingId": dynamic_value,
            "response": response_second_api.text
        }

        results.append(result)

        print("Response for {}: {}".format(dynamic_value, response_second_api.text))

output_data = "output.csv"
output_csv = os.path.join(cwd, output_data)
with open(output_csv, 'w', newline='') as csvfile:
    fieldnames = ["trackingId", "response"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()

    for result in results:
        writer.writerow(result)
