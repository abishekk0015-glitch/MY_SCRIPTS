#!/usr/bin/env python3
from __future__ import print_function

import argparse
import csv
import json
import sys
import time
import uuid

import requests

VARADHI_BASE = "http://10.24.0.209"
TOPIC = "ekl.fsd.shipping.update.prod"
SUB = "consolidate_update_shipment"
ERROR_SUBSTR = "Error While calling shipping dobby"
EVENT_NAME = "fsd_outscan"
RESPONSE_CODE = 500
BATCH_SIZE = 100

SHIPPING_BASE_120 = "http://10.24.1.120"
SHIPPING_BASE_19 = "http://10.24.1.19"
SHIPPING_DETAILS = SHIPPING_BASE_19 + "/shipments/get_all_shipment_details?vendor_tracking_id={}"
PATCH_INSCAN = SHIPPING_BASE_120 + "/shipments/patch_events/inscan_outscan"
UNSIDELINE_URL = "{}/topics/{}/subscriptions/{}/messages/actions/un-sideline".format(
    VARADHI_BASE, TOPIC, SUB
)

EKART_SECRET_CODE = "G[8428B5MI$UqF@iX/4d0>kI.9d3>T"

EXPECTED_STATE = "pickup_complete"
RECEIVED_STATUS = "received"


def get_varadhi_token():
    url = "https://service.authn-prod.fkcloud.in/v3/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": "script",
        "client_secret": "7+XDA7BR7RZ08FCLyH1d2AFjyW5NuKAUd5px7Jk87w/bFNmv",
        "target_client_id": "http://10.24.0.209:80",
    }
    headers = {"content-type": "application/x-www-form-urlencoded"}
    token = requests.post(url, data=payload, headers=headers).json()
    return "Bearer " + str(token["access_token"])


def get_shipping_token():
    url = "https://service.authn-prod.fkcloud.in/v3/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": "r2d2",
        "client_secret": "2eY3BjxIEr/pj26SykHyIcuOo3ZUoeCMFT/F/FJ3+ic460c3",
        "target_client_id": "fkl-shipping",
    }
    headers = {"content-type": "application/x-www-form-urlencoded"}
    token = requests.post(url, data=payload, headers=headers).json()
    return "Bearer " + str(token["access_token"])


def varadhi_headers():
    return {"Authorization": get_varadhi_token(), "Content-Type": "application/json"}


def shipping_headers():
    return {
        "Authorization": get_shipping_token(),
        "Content-Type": "application/json",
        "EKART_SECRET_CODE": EKART_SECRET_CODE,
    }


def message_count(headers):
    url = "{}/topics/{}/subscriptions/{}/messages/actions/count?type=sideline".format(
        VARADHI_BASE, TOPIC, SUB
    )
    return int(requests.get(url, headers=headers).text)


def get_sidelined_messages(headers, offset, limit):
    url = (
        "{}/topics/{}/subscriptions/{}/messages"
        "?sidelined=true&response_code={}&offset={}&limit={}"
    ).format(VARADHI_BASE, TOPIC, SUB, RESPONSE_CODE, offset, limit)
    for _ in range(5):
        try:
            return requests.get(url, headers=headers).json()
        except ValueError:
            time.sleep(2)
    raise RuntimeError("failed to fetch messages at offset {}".format(offset))


def parse_payload(raw_message):
    if isinstance(raw_message, dict):
        return raw_message
    return json.loads(raw_message)


def event_of(msg, payload):
    event = payload.get("event")
    if event:
        return str(event).lower()
    headers = msg.get("http_headers") or {}
    return str(headers.get("X_EVENT_NAME") or headers.get("x_event_name") or "").lower()


def fetch_stuck_fsd_outscan(batch_size):
    headers = varadhi_headers()
    total = message_count(headers)
    print("sidelined count (all codes): {}".format(total))

    rows = []
    seen = set()
    offset = 0
    while offset < total + batch_size:
        messages = get_sidelined_messages(headers, offset, batch_size)
        if not messages:
            break
        for msg in messages:
            body = str(msg.get("http_response_body") or "")
            if ERROR_SUBSTR not in body:
                continue
            try:
                payload = parse_payload(msg.get("message"))
            except Exception:
                payload = {}
            if event_of(msg, payload) != EVENT_NAME:
                continue
            vtid = payload.get("vendor_tracking_id") or msg.get("group_id")
            if not vtid or vtid in seen:
                continue
            seen.add(vtid)
            rows.append(
                {
                    "vendor_tracking_id": vtid,
                    "group_id": msg.get("group_id") or vtid,
                    "message_id": msg.get("message_id"),
                    "event": EVENT_NAME,
                    "hub_id": payload.get("hub_id"),
                    "merchant_reference_id": payload.get("merchant_reference_id"),
                }
            )
        offset += batch_size
        print("scanned offset={}, fsd_outscan dobby-500 matched={}".format(offset, len(rows)))
    return rows


def _extract_state(body):
    items = body if isinstance(body, list) else [body]
    for item in items:
        if not isinstance(item, dict):
            continue
        state = (
            item.get("state")
            or item.get("current_state")
            or item.get("status")
            or (item.get("shipment") or {}).get("state")
            or (item.get("shipment") or {}).get("status")
        )
        if state:
            return state
    return None


def _history_has_received(details_body):
    items = details_body if isinstance(details_body, list) else [details_body]
    for details in items:
        if not isinstance(details, dict):
            continue
        history = details.get("shipment_history") or []
        for h in history:
            if not isinstance(h, dict):
                continue
            status = (
                h.get("new_status")
                or h.get("status")
                or h.get("shipment_status")
                or ""
            )
            if str(status).lower() == RECEIVED_STATUS:
                return True
    return False


def shipping_eligible(vtid, ship_hdrs):
    try:
        r = requests.get(SHIPPING_DETAILS.format(vtid), headers=ship_hdrs, timeout=15)
        if r.status_code != 200:
            return False, None, None, "shipping_get_http_{}".format(r.status_code)
        details_json = r.json()
        state = _extract_state(details_json)
        has_received = _history_has_received(details_json)
    except Exception as e:
        return False, None, None, "shipping_get_err:{}".format(e)

    if str(state or "").lower() != EXPECTED_STATE:
        return False, state, has_received, "state_not_pickup_complete"
    if has_received:
        return False, state, True, "received_present_in_history"
    return True, state, False, "eligible"


def mock_inscan(vtid, merchant_reference_id, headers):
    body = {
        "vendor_tracking_id": vtid,
        "merchant_reference_id": merchant_reference_id or vtid,
        "current_state": "nil",
        "patched_id": "oncall-fsd-outscan-{}-{}".format(vtid, uuid.uuid4().hex[:8]),
    }
    r = requests.post(PATCH_INSCAN, headers=headers, data=json.dumps(body), timeout=30)
    return r.status_code, r.text[:500]


def unsideline_group_ids(group_ids, batch_size):
    if not group_ids:
        print("nothing to unsideline")
        return
    seen = set()
    unique = []
    for g in group_ids:
        if g and g not in seen:
            seen.add(g)
            unique.append(g)

    for i in range(0, len(unique), batch_size):
        batch = unique[i : i + batch_size]
        headers = varadhi_headers()
        model = {"group_ids": batch}
        print("unsidelining batch {}-{} size={}".format(i, i + len(batch), len(batch)))
        resp = requests.post(UNSIDELINE_URL, headers=headers, data=json.dumps(model), timeout=60)
        print("unsideline status={} body={}".format(resp.status_code, resp.text[:300]))
        time.sleep(2)


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def main():
    parser = argparse.ArgumentParser(description="Fix fsd_outscan dobby-500 sidelined shipments")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--out", default="/tmp/fsd_outscan_dobby_500_fix.csv")
    parser.add_argument("--unsideline-skipped", action="store_true")
    args = parser.parse_args()
    batch_size = args.batch_size

    print("=== Step 1: fetch stuck fsd_outscan dobby-500 ===")
    stuck = fetch_stuck_fsd_outscan(batch_size)
    print("stuck fsd_outscan count={}".format(len(stuck)))
    if not stuck:
        print("nothing to do")
        return 0

    print("=== Step 2: filter pickup_complete + no received ===")
    ship_hdrs = shipping_headers()
    eligible = []
    skipped = []
    for row in stuck:
        ok, state, has_received, reason = shipping_eligible(row["vendor_tracking_id"], ship_hdrs)
        row["shipping_state"] = state
        row["has_received"] = has_received
        row["filter_reason"] = reason
        if ok:
            eligible.append(row)
            print("ELIGIBLE", row["vendor_tracking_id"], state)
        else:
            skipped.append(row)
            print("SKIP", row["vendor_tracking_id"], state, reason)
        if args.limit and len(eligible) >= args.limit:
            break

    print("eligible={}, skipped={}".format(len(eligible), len(skipped)))

    patched_ok = []
    if args.apply:
        print("=== Step 3: mock inscan ===")
        for batch in chunks(eligible, batch_size):
            for row in batch:
                vtid = row["vendor_tracking_id"]
                code, text = mock_inscan(vtid, row.get("merchant_reference_id"), ship_hdrs)
                row["patch_status"] = code
                row["patch_response"] = text
                if code == 200:
                    patched_ok.append(row)
                    print("PATCH_OK", vtid)
                else:
                    print("PATCH_FAIL", vtid, code, text)
            time.sleep(1)

        print("=== Step 4: unsideline ===")
        to_unsideline = [r["group_id"] for r in patched_ok]
        if args.unsideline_skipped:
            to_unsideline.extend(r["group_id"] for r in skipped)
        unsideline_group_ids(to_unsideline, batch_size)
    else:
        print("DRY-RUN: skip patch + unsideline. Re-run with --apply to mutate.")
        for row in eligible:
            row["patch_status"] = "dry_run"
            row["patch_response"] = ""

    report = []
    for row in eligible:
        row = dict(row)
        row["action"] = "eligible"
        report.append(row)
    for row in skipped:
        row = dict(row)
        row["action"] = "skipped"
        report.append(row)

    if report:
        fieldnames = sorted({k for r in report for k in r.keys()})
        with open(args.out, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report)
        print("wrote {} rows -> {}".format(len(report), args.out))

    print(
        "done. stuck={} eligible={} patched_ok={}".format(
            len(stuck), len(eligible), len(patched_ok) if args.apply else 0
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
