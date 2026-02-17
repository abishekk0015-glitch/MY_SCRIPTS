#!/bin/bash
set -x

CURR_DATE=$(date +"%b-%d")
CURR_TIME=$(date +"%Y-%m-%d_%H-%M-%S")
DATE=`date +"%F" `
CURR_DATE=`date +"%b-%d"`
LAST_DATE=`date +\%Y-\%m-\%d -d "-1 days"`

CWD="/mnt/archive/test2"

rm -f $CWD/*.csv

srms_user="v-ekl--0p2iTiS2H"
srms_pwd="qMsNrAM9nX-mkQiqWwSK"
srms_db="srms"
srms_ip="10.24.40.139"
srms_port="4000"

export MAILFROM="ext-oncall <ekart-ext-oncall@flipkart.com>"
export MAILTO="bikrant.sahoo@flipkart.com,sonalipanda.d@flipkart.com,abisheka.vc@flipkart.com"
export SUBJECT="Large billing report | ${CURR_DATE}"

# Define date range for the queries
first_date="2026-01-26"
last_date="2026-02-16"

# Original SRMS queries (These queries fetch tracking data)
srms_query="select distinct client_service_request_id , sr_id from service_requests s where s.client_id not in ('flipkart') and s.sr_type in ('NONFA_FORWARD_LARGE_E2E_EKART','NONFA_RVP_LARGE_E2E_EKART')
AND s.created_at >= '2023-06-01 00:00:00' AND s.updated_at between '$first_date 00:00:00' and '$last_date 23:59:59';"

srms_query_2="select distinct client_service_request_id , sr_id from service_requests s where s.client_id not in ('flipkart') and s.sr_type in ('NONFA_FORWARD_LARGE_E2E_EKART','NONFA_RVP_LARGE_E2E_EKART')
AND s.created_at between '$first_date 00:00:00' and '$last_date 23:59:59';"

srms_query_3="select distinct client_service_request_id , sr_id from service_requests s where s.client_id not in ('flipkart') and s.sr_type in ('NONFA_FORWARD_LARGE_E2E_EKART','NONFA_RVP_LARGE_E2E_EKART')
AND s.updated_at between '$first_date 00:00:00' and '$last_date 23:59:59';"

echo "$srms_query" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' > $CWD/srms_total_temp_1.csv
echo "$srms_query_2" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' > $CWD/srms_total_temp_2.csv
echo "$srms_query_3" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' > $CWD/srms_total_temp_3.csv

cat $CWD/srms_total_temp_1.csv $CWD/srms_total_temp_2.csv $CWD/srms_total_temp_3.csv > $CWD/srms_total_temp_4.csv

awk -F ',' '!seen[$1]++' $CWD/srms_total_temp_4.csv > $CWD/srms_total_temp.csv

awk -F ',' '{print $2}' $CWD/srms_total_temp.csv > $CWD/srms_total.csv

# Batching logic to prevent database memory errors
split -l 5000 $CWD/srms_total.csv "$CWD/srms_batch_"

for f in "$CWD"/srms_batch_*; do
    id_list_batch=$(awk -F, '{printf "\x27%s\x27,", $1}' "$f" | sed 's/,$//')

    if [[ -z "$id_list_batch" ]]; then
      echo "Skipping empty batch file: $f"
      continue
    fi

    xquery="select distinct SUBSTRING_INDEX(SUBSTRING_INDEX(blobs, '\"name\":\"vendor_tracking_id\",\"type\":\"string\",\"value\":\"', -1), '\"', 1) as vendor_tracking_id from sr_data_non_queryable where shard_key IN (${id_list_batch});"

    echo "$xquery" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' >> $CWD/srid_list_all_batches.csv
done

awk -F ',' '!seen[$1]++' "$CWD/srid_list_all_batches.csv" > $CWD/srid_list.csv

awk '!/vendor_tracking_id/' $CWD/srid_list.csv > $CWD/srid_list1.csv

echo "merchant_id,lr_id,tracking_id,client_id,shipment_type,shipment_status,payment_type,docket_id,docket_status,seller_declared_length,seller_declared_breadth,seller_declared_height,creation_date,vol_weight,shipment_weight,source_pincode,destination_pincode,source_state,destination_state,value,amount_to_collect" >"$CWD/output.csv"
while IFS= read -r tracking_id; do
    if [[ -z "$tracking_id" ]]; then continue; fi
    curl_output=$(curl --location --request GET "http://10.24.1.7/service-requests/internal/track?trackingId=${tracking_id}")
    echo "$curl_output" >$CWD/response.json

    service_request_type=$(jq -r '.payload.serviceRequest.serviceRequestType' $CWD/response.json)
    if [[ "$service_request_type" == "NONFA_FORWARD_LARGE_E2E_EKART" ]]; then
        shipment_type="FORWARD"
        a=$(jq -r '.payload.serviceRequestsTrackData[]' $CWD/response.json | grep "ShipmentRtoConfirmed" | wc -l)
        if [[ $a == 1 ]]; then
            shipment_type="RTO"
        fi
    else
        shipment_type="REVERSE"
    fi

    Status=$(jq -r '.payload.serviceRequest.status' $CWD/response.json)
    case "$Status" in
        "OutForPickupEvent") if [[ "$shipment_type" == "REVERSE" ]]; then shipment_status="out_for_pickup"; else shipment_status="pickup_out_for_pickup"; fi ;;
        "PickupRescheduledEvent") shipment_status="pickup_reattempt" ;;
        "PickupReceived") shipment_status="shipment_pickup_complete" ;;
        "PickupCancelled"|"PickupCancel") shipment_status="pickup_cancelled" ;;
        "InScanAtHub") if [[ "$shipment_type" == "REVERSE" ]]; then shipment_status="return_received"; else shipment_status="received"; fi ;;
        "InscannedAtDH") shipment_status="received_at_dh" ;;
        "ShipmentOutForDelivery") shipment_status="shipment_out_for_delivery" ;;
        "ShipmentUndeliveredAttempted") shipment_status="shipment_undelivered_attempted" ;;
        "ShipmentDelivered") shipment_status="shipment_delivered" ;;
        "ShipmentLost") shipment_status="shipment_lost" ;;
        "ShipmentRtoConfirmed") shipment_status="shipment_rto_confirmed" ;;
        "ShipmentRtoCompleted") shipment_status="shipment_rto_completed" ;;
        "RtoCancelled") shipment_status="shipment_rto_cancelled" ;;
        "NotPickedAttemptedEvent") shipment_status="pickup_not_picked_attempted" ;;
        "NotPickedNotAttemptedEvent") shipment_status="pickup_not_picked_unattempted" ;;
        "PickedComplete") shipment_status="pickup_done" ;;
        "ReturnOutForDelivery") shipment_status="return_out_for_delivery" ;;
        "ReturnUndeliveredAttempted") shipment_status="return_undelivered_attempted" ;;
        "ShipmentRvpCompleted") shipment_status="return_delivered" ;;
        *) shipment_status="Created" ;;
    esac

    payment_type=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.payment.paymentDetails[].type' $CWD/response.json)
    docket_id=$(jq -r '.payload.serviceRequest.clientServiceRequestId' $CWD/response.json)
    docket_status=$(jq -r '.payload.serviceRequest.status' $CWD/response.json)
    seller_declared_length=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.length.value' $CWD/response.json)
    seller_declared_breadth=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.breadth.value' $CWD/response.json)
    seller_declared_height=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.height.value' $CWD/response.json)
    creation_date=$(jq -r '.payload.serviceRequest.serviceStartDate' $CWD/response.json)
    vol_weight=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.volumetricWeight.value' $CWD/response.json)
    shipment_weight=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentWeight.value' $CWD/response.json)
    source_pincode=$(jq -r '.payload.serviceRequest.serviceRequestData.source.address.pincode' $CWD/response.json)
    destination_pincode=$(jq -r '.payload.serviceRequest.serviceRequestData.destination.address.pincode' $CWD/response.json)
    source_state=$(jq -r '.payload.serviceRequest.serviceRequestData.source.address.state' $CWD/response.json)
    destination_state=$(jq -r '.payload.serviceRequest.serviceRequestData.destination.address.state' $CWD/response.json)
    value=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.payment.totalAmount.value' $CWD/response.json)
    lr_id=${tracking_id}
    merchant_id=${tracking_id}
    client_id=$(jq -r '.payload.serviceRequest.clientId' $CWD/response.json)
    amount_to_collect=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.payment.amountToCollect.value' $CWD/response.json)

    echo -e "$merchant_id,$lr_id,$tracking_id,$client_id,$shipment_type,$shipment_status,$payment_type,$docket_id,$docket_status,$seller_declared_length,$seller_declared_breadth,$seller_declared_height,$creation_date,$vol_weight,$shipment_weight,$source_pincode,$destination_pincode,$source_state,$destination_state,$value,$amount_to_collect" >> $CWD/output.csv

done <$CWD/srid_list1.csv

awk '!/vendor_tracking_id/' $CWD/output.csv > $CWD/output_final.csv
awk '!/sr_id/' $CWD/srms_total.csv > $CWD/srs_final.csv

# Loop over the tracking IDs and process each batch of 5000
split -l 5000 $CWD/srs_final.csv $CWD/data

n=1
for f in $CWD/data*; do
    SPLIT_TRACKING_IDS=$(awk -F, '{print "'\''" $1 "'\''"}' $f | paste -sd,)
    if [[ -z "$SPLIT_TRACKING_IDS" ]]; then continue; fi
    pquery="SELECT 
          MAX(SUBSTRING_INDEX(SUBSTRING_INDEX(sd.blobs, '\"name\":\"vendor_tracking_id\",\"type\":\"string\",\"value\":\"', -1), '\"', 1)) AS merchant_id,
          MIN(CASE WHEN std.status = 'PickupReceived' THEN std.time END) AS pickup_date,
          MIN(CASE WHEN std.status = 'InscannedAtDH' THEN std.time END) AS dh_in_scan_date,
          MIN(CASE WHEN std.status = 'ShipmentRtoConfirmed' THEN std.time END) AS rto_request_date,
          MIN(CASE WHEN std.status = 'ShipmentDelivered' THEN std.time END) AS delivered_date,
          MAX(SUBSTRING_INDEX(SUBSTRING_INDEX(sd.blobs, '\"name\":\"zone_type\",\"type\":\"string\",\"value\":\"', -1), '\"', 1)) AS zone,
          MAX(s.updated_at) as shipment_status_date
          FROM service_requests s
          JOIN sr_tracking_data std ON s.sr_id = std.shard_key
          LEFT JOIN sr_data_non_queryable sd ON s.sr_id = sd.shard_key
          WHERE s.sr_id IN (${SPLIT_TRACKING_IDS})
          GROUP BY s.sr_id;"

    echo "$pquery" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' >$CWD/sr_1_total_$n.csv
    ((n++))
    echo "Batch $n done"
done

cat $CWD/sr_1_total_*.csv > $CWD/sr_total_unique.csv
awk -F ',' '!seen[$1]++' $CWD/sr_total_unique.csv > $CWD/sr_total.csv

join -1 1 -2 1 -t, -o 1.3,1.2,1.1,1.4,2.2,2.3,2.4,2.5,2.6,1.5,1.6,2.7,1.7,1.8,1.9,1.10,1.11,1.12,1.13,1.14,1.15,1.16,1.17,1.18,1.19,1.20,1.21 <(sort -k1 $CWD/output_final.csv) <(sort -k1 $CWD/sr_total.csv)> $CWD/Large_billing_data.csv

uniq -i $CWD/Large_billing_data.csv > $CWD/temp_file && mv $CWD/temp_file $CWD/Large_billing_data.csv
awk -F ',' '!seen[$1]++' $CWD/Large_billing_data.csv > $CWD/output_1.csv && mv $CWD/output_1.csv $CWD/Large_billing_data.csv

final_header="tracking_id,lr_id,merchant_id,client_id,pickup_date,dh_in_scan_date,rto_request_date,delivered_date,zone,shipment_type,shipment_status,shipment_status_date,payment_type,docket_id,docket_status,seller_declared_length,seller_declared_breadth,seller_declared_height,creation_date,vol_weight,shipment_weight,source_pincode,destination_pincode,source_state,destination_state,value,amount_to_collect"
(echo "$final_header"; awk '!/^tracking_id/' $CWD/Large_billing_data.csv) > $CWD/Large_billing_data_with_header.csv
mv "$CWD/Large_billing_data_with_header.csv" "$CWD/Large_billing_data.csv"

awk 'BEGIN {FS=OFS=","} { $20=""; sub(/,,/, ","); print }' $CWD/Large_billing_data.csv > $CWD/Large_billing_data_new_file.csv
awk -F',' '$12 == "shipment_rto_completed" || $12 == "shipment_rto_confirmed" || $12 == "return_out_for_delivery" || $12 == "return_undelivered_attempted" { $11 = "RTO" } 1' OFS=',' $CWD/Large_billing_data_new_file.csv > $CWD/h.csv
awk -F ',' '$5 !~ /2026-02-17/' $CWD/h.csv > $CWD/Large_final.csv
