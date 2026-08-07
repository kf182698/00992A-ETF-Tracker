"""Build the static JSON feed consumed by the 00992A tracker site."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_snapshot(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        rows = csv.DictReader(file)
        holdings = []
        for row in rows:
            code = (row.get("股票代號") or "").strip()
            if not code:
                continue
            try:
                units = int(float((row.get("股數") or "0").replace(",", "")))
                weight = float((row.get("持股權重") or "0").replace("%", "").replace(",", ""))
            except ValueError:
                continue
            holdings.append({"code": code, "name": (row.get("股票名稱") or code).strip(), "units": units, "weight": weight})
    return sorted(holdings, key=lambda item: (-item["weight"], item["code"]))


def snapshot_files() -> list[tuple[str, Path]]:
    files: dict[str, Path] = {}
    for folder in (ROOT / "data", ROOT / "data_snapshots"):
        if not folder.exists():
            continue
        for path in folder.glob("*.csv"):
            date = path.stem
            if len(date) == 10 and date[4] == "-" and date[7] == "-":
                files[date] = path
    return sorted(files.items())


def flag(previous: dict | None, current: dict | None, delta_units: int, delta_weight: float) -> str:
    if previous is None and current is not None:
        return "新增持股"
    if previous is not None and current is None:
        return "退出持股"
    if previous is None or current is None:
        return "無變動"
    relative_units = abs(delta_units) / max(previous["units"], 1)
    if delta_units > 0 and (relative_units >= 0.1 or delta_weight >= 0.25):
        return "大幅加碼"
    if delta_units < 0 and (relative_units >= 0.1 or delta_weight <= -0.25):
        return "大幅減持"
    return "仍持有"


def main() -> None:
    snapshots = [(date, read_snapshot(path)) for date, path in snapshot_files()]
    if len(snapshots) < 2:
        raise SystemExit("Need at least two snapshot files to build the website feed.")

    histories: dict[str, list[dict]] = defaultdict(list)
    names: dict[str, str] = {}
    events: dict[str, list[dict]] = defaultdict(list)
    previous: dict[str, dict] = {}

    for date, holdings in snapshots:
        current = {item["code"]: item for item in holdings}
        for rank, item in enumerate(holdings, start=1):
            names[item["code"]] = item["name"]
            histories[item["code"]].append({"date": date, "units": item["units"], "weight": round(item["weight"], 4), "rank": rank})

        for code in set(previous) | set(current):
            before, after = previous.get(code), current.get(code)
            before_units = before["units"] if before else 0
            before_weight = before["weight"] if before else 0
            after_units = after["units"] if after else 0
            after_weight = after["weight"] if after else 0
            delta_units = after_units - before_units
            delta_weight = round(after_weight - before_weight, 4)
            if previous and (delta_units or delta_weight):
                kind = "新增持股" if before is None else "退出持股" if after is None else "加碼" if delta_units > 0 or delta_weight > 0 else "減碼"
                events[code].append({"date": date, "type": kind, "deltaUnits": delta_units, "deltaWeight": delta_weight, "rank": next((index + 1 for index, item in enumerate(holdings) if item["code"] == code), None)})
        previous = current

    latest_date, latest = snapshots[-1]
    previous_date, before_latest = snapshots[-2]
    latest_by_code = {item["code"]: item for item in latest}
    before_by_code = {item["code"]: item for item in before_latest}
    stocks = []
    for code, history in histories.items():
        current = latest_by_code.get(code)
        before = before_by_code.get(code)
        delta_units = (current["units"] if current else 0) - (before["units"] if before else 0)
        delta_weight = round((current["weight"] if current else 0) - (before["weight"] if before else 0), 4)
        stocks.append({
            "code": code, "name": names[code], "history": history,
            "events": events[code], "firstDate": history[0]["date"], "firstWeight": history[0]["weight"], "firstUnits": history[0]["units"], "firstRank": history[0]["rank"],
            "lastDate": history[-1]["date"], "isHeld": current is not None,
            "current": {"date": latest_date, "units": current["units"], "weight": round(current["weight"], 4), "rank": next(index + 1 for index, item in enumerate(latest) if item["code"] == code)} if current else None,
            "deltaUnits": delta_units, "deltaWeight": delta_weight,
            "maxWeight": max(history, key=lambda item: item["weight"]), "maxUnits": max(history, key=lambda item: item["units"]),
            "flag": flag(before, current, delta_units, delta_weight),
        })

    stocks.sort(key=lambda item: (not item["isHeld"], -(item["current"] or {"weight": 0})["weight"], item["code"]))
    latest_holdings = [{**item, "rank": index + 1, "weight": round(item["weight"], 4)} for index, item in enumerate(latest)]
    recent_events = sorted(({"code": stock["code"], "name": stock["name"], **event} for stock in stocks for event in stock["events"]), key=lambda item: item["date"], reverse=True)[:80]
    stats = {"holdings": len(latest), "added": 0, "removed": 0, "increased": 0, "reduced": 0}
    for stock in stocks:
        if stock["flag"] == "新增持股": stats["added"] += 1
        elif stock["flag"] == "退出持股": stats["removed"] += 1
        elif stock["flag"] == "大幅加碼": stats["increased"] += 1
        elif stock["flag"] == "大幅減持": stats["reduced"] += 1

    payload = {"source": "kf182698/00992A-ETF-Tracker", "latestDate": latest_date, "previousDate": previous_date, "snapshotCount": len(snapshots), "latestHoldings": latest_holdings, "stocks": stocks, "recentEvents": recent_events, "stats": stats}
    output = ROOT / "web" / "etf-tracker.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


if __name__ == "__main__":
    main()
