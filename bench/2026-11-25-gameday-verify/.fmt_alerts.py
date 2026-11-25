import json, sys
try:
    alerts = json.load(sys.stdin)["data"]["alerts"]
except Exception:
    alerts = []
names = sorted(a["labels"]["alertname"] + ("/" + a["labels"]["backend"] if "backend" in a["labels"] else "") + "=" + a["state"] for a in alerts)
print(",".join(names))
