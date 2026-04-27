import json
import time
import re
import requests
from requests.auth import HTTPBasicAuth

# ---------------- CONFIG ----------------
YAMCS_URL = "http://localhost:8090"
INSTANCE = "general"

USERNAME = "admin"
PASSWORD = "admin"

# 👉 PUT FULL PATH HERE IF NEEDED
INPUT_FILE = "timeline_items.json"
# ---------------------------------------

session = requests.Session()
session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
session.headers.update({"Content-Type": "application/json"})

DUR_RE = re.compile(r"^\s*(\d+)\s*s\s*$", re.IGNORECASE)

def parse_duration(item):
    # Case 1: duration_s exists
    if "duration_s" in item and item["duration_s"] is not None:
        try:
            return int(item["duration_s"])
        except:
            pass

    # Case 2: "duration": "3600s"
    dur = item.get("duration")
    if isinstance(dur, str):
        m = DUR_RE.match(dur)
        if m:
            return int(m.group(1))

    return 0

def to_yamcs_item(item):
    start = item.get("start")
    if not start:
        raise ValueError("Missing start")

    dur_s = parse_duration(item)

    # 🔴 CRITICAL: never allow 0
    if dur_s <= 0:
        dur_s = 3600
    tags = item.get("tags", [])
    tags.append("import_v1")
    payload = {
        "source": "rdb",
        "name": item.get("name", "Untitled"),
        "type": "EVENT",
        "start": start,
        "duration": f"{dur_s}s",
        "tags": list(set(tags)),
        "description": item.get("description", ""),
        "properties": item.get("properties", {}),
    }

    # 🚫 DO NOT SEND THESE
    # payload["id"] = ...
    # payload["groupId"] = ...

    return payload

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        items = json.load(f)

    url = f"{YAMCS_URL}/api/timeline/{INSTANCE}/items"

    ok = 0
    fail = 0
    failures = []

    for i, item in enumerate(items, start=1):
        try:
            payload = to_yamcs_item(item)
            r = session.post(url, json=payload)

            if r.ok:
                ok += 1
            else:
                fail += 1
                failures.append({
                    "index": i,
                    "status": r.status_code,
                    "response": r.text[:300],
                    "name": payload["name"],
                    "start": payload["start"],
                    "duration": payload["duration"]
                })

        except Exception as e:
            fail += 1
            failures.append({
                "index": i,
                "error": str(e)
            })

        if i % 25 == 0:
            time.sleep(0.2)

    print(f"Imported OK: {ok}, failed: {fail}")

    if failures:
        with open("import_failures.json", "w", encoding="utf-8") as f:
            json.dump(failures, f, indent=2)
        print("Wrote import_failures.json")

if __name__ == "__main__":
    main()
