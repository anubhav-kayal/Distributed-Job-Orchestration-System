"""Run every query on the Grafana dashboard against Prometheus and report which return
data, so a typo in a metric name or label can't silently leave a panel empty.

    uv run python scripts/check_dashboard.py [--prometheus localhost:59090]
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DASHBOARD = Path(__file__).parent.parent / "deploy" / "grafana" / "dashboards" / "djos.json"


def query(prometheus: str, expr: str) -> list:
    url = f"http://{prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = json.load(resp)
    if body["status"] != "success":
        raise RuntimeError(body)
    return body["data"]["result"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus", default="localhost:59090")
    args = parser.parse_args()

    empty = 0
    for panel in json.loads(DASHBOARD.read_text())["panels"]:
        for t in panel["targets"]:
            series = query(args.prometheus, t["expr"])
            mark = "ok " if series else "EMPTY"
            empty += not series
            label = t.get("legendFormat") or ""
            print(f"  {mark} {panel['title'][:34]:<34} {label[:22]:<22} {len(series)} series")
    print("all panels have data" if not empty else f"{empty} queries returned no data")
    return 1 if empty else 0


if __name__ == "__main__":
    sys.exit(main())
