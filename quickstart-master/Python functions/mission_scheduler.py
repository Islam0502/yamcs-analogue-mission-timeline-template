#!/usr/bin/env python3
"""
Mission-agnostic analogue mission timeline scheduler.

Produces Yamcs Timeline CreateItemRequest JSON objects compatible with:
POST /api/timeline/{instance}/items
"""
from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def parse_hhmm(s: str) -> int:
    """Parse 'HH:MM' to minutes since midnight."""
    hh, mm = s.strip().split(":")
    return int(hh) * 60 + int(mm)


def parse_date_yyyy_mm_dd(s: str) -> date:
    return date.fromisoformat(s)


def ensure_tzinfo(tz: str) -> Any:
    if ZoneInfo is None:
        return timezone.utc
    return ZoneInfo(tz)


def to_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt.isoformat(timespec="seconds")


def duration_seconds_string(seconds: int) -> str:
    return f"{int(seconds)}s"


def slotify(minutes: int, slot_minutes: int) -> int:
    if minutes % slot_minutes != 0:
        return (minutes + slot_minutes - 1) // slot_minutes
    return minutes // slot_minutes


def unslotify(slots: int, slot_minutes: int) -> int:
    return int(slots) * slot_minutes


@dataclass(frozen=True)
class CrewLane:
    lane: str
    role: str = ""


@dataclass(frozen=True)
class TimeWindow:
    start_minute: int
    end_minute: int


@dataclass(frozen=True)
class RecurringBlock:
    name: str
    start_hhmm: str
    end_hhmm: str
    participants: List[str]
    days: str = "DAILY"
    description: str = ""
    priority: int = 100


@dataclass(frozen=True)
class ActivityRequest:
    name: str
    duration_min: int
    duration_max: Optional[int] = None
    participants: List[str] = field(default_factory=list)
    day: Optional[int] = None
    windows: Optional[List[Dict[str, str]]] = None
    type: str = "EVENT"
    kind: Optional[str] = None
    relax_after_min: Optional[int] = None
    description: str = ""
    tags_extra: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScheduledActivity:
    activity: ActivityRequest
    start_slot: int
    duration_slots: int
    lanes: List[str]


@dataclass(frozen=True)
class LaneFixedSegment:
    lane: str
    start_slot: int
    end_slot: int
    request: ActivityRequest


def expand_days_selector(selector: str, mission_days: int) -> List[int]:
    selector = selector.strip().upper()
    if selector == "DAILY":
        return list(range(mission_days))
    if selector.startswith("DAYS:"):
        parts = selector.split(":", 1)[1].split(",")
        out = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            out.append(int(p))
        return [d for d in out if 0 <= d < mission_days]
    raise ValueError(f"Unsupported days selector: {selector}")


def participants_to_lanes(participants: Sequence[str], lanes: List[CrewLane]) -> List[str]:
    lane_names = [l.lane for l in lanes]
    if any(p.upper() == "ALL" for p in participants):
        return lane_names
    return [p for p in participants if p in lane_names]


def build_default_windows() -> List[TimeWindow]:
    return [
        TimeWindow(parse_hhmm("09:00"), parse_hhmm("12:00")),
        TimeWindow(parse_hhmm("13:00"), parse_hhmm("19:00")),
        TimeWindow(parse_hhmm("20:00"), parse_hhmm("22:30")),
    ]


def subtract_interval(pieces: List[Tuple[int, int]], blocked_start: int, blocked_end: int) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    for start, end in pieces:
        if blocked_end <= start or blocked_start >= end:
            out.append((start, end))
            continue
        if start < blocked_start:
            out.append((start, blocked_start))
        if blocked_end < end:
            out.append((blocked_end, end))
    return [(s, e) for (s, e) in out if e > s]


def preprocess_fixed_blocks(
    *,
    mission_days: int,
    slot_minutes: int,
    lanes: List[CrewLane],
    fixed_blocks: List[RecurringBlock],
) -> Tuple[Dict[str, List[Tuple[int, int]]], List[ScheduledActivity], Dict[str, List[Tuple[int, int]]]]:
    """Split lower-priority recurring blocks around higher-priority ones on the same lane."""
    day_slots = slotify(24 * 60, slot_minutes)
    lane_segments: Dict[str, List[LaneFixedSegment]] = {l.lane: [] for l in lanes}

    occurrences: List[Tuple[int, int, int, int, List[str], ActivityRequest]] = []
    for index, block in enumerate(fixed_blocks):
        days = expand_days_selector(block.days, mission_days)
        start_m = parse_hhmm(block.start_hhmm)
        end_m = parse_hhmm(block.end_hhmm)
        wraps = end_m <= start_m
        lanes_for_block = participants_to_lanes(block.participants, lanes)
        for d in days:
            start_slot = d * day_slots + slotify(start_m, slot_minutes)
            end_slot = (d + 1) * day_slots + slotify(end_m, slot_minutes) if wraps else d * day_slots + slotify(end_m, slot_minutes)
            req = ActivityRequest(
                name=block.name,
                duration_min=unslotify(end_slot - start_slot, slot_minutes),
                participants=block.participants,
                day=d,
                windows=[{"from": block.start_hhmm, "to": block.end_hhmm}],
                type="EVENT",
                description=block.description,
            )
            occurrences.append((block.priority, index, start_slot, end_slot, lanes_for_block, req))

    occurrences.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))

    for priority, index, start_slot, end_slot, lanes_for_block, req in occurrences:
        for lane in lanes_for_block:
            pieces = [(start_slot, end_slot)]
            for seg in lane_segments[lane]:
                pieces = subtract_interval(pieces, seg.start_slot, seg.end_slot)
                if not pieces:
                    break
            for piece_start, piece_end in pieces:
                lane_segments[lane].append(LaneFixedSegment(lane, piece_start, piece_end, req))
            lane_segments[lane].sort(key=lambda s: (s.start_slot, s.end_slot, s.request.name))

    occupied: Dict[str, List[Tuple[int, int]]] = {l.lane: [] for l in lanes}
    fixed_output: List[ScheduledActivity] = []
    fixed_windows_by_lane: Dict[str, List[Tuple[int, int]]] = {l.lane: [] for l in lanes}
    for lane, segments in lane_segments.items():
        for seg in segments:
            occupied[lane].append((seg.start_slot, seg.end_slot))
            fixed_windows_by_lane[lane].append((seg.start_slot, seg.end_slot))
            fixed_output.append(
                ScheduledActivity(
                    activity=seg.request,
                    start_slot=seg.start_slot,
                    duration_slots=seg.end_slot - seg.start_slot,
                    lanes=[lane],
                )
            )
    for lane in occupied:
        occupied[lane].sort()
        fixed_windows_by_lane[lane].sort()
    fixed_output.sort(key=lambda x: (x.start_slot, x.activity.name, x.lanes[0]))
    return occupied, fixed_output, fixed_windows_by_lane


def greedy_schedule(
    *,
    mission_days: int,
    slot_minutes: int,
    lanes: List[CrewLane],
    fixed_blocks: List[RecurringBlock],
    requests: List[ActivityRequest],
) -> List[ScheduledActivity]:
    """Greedy first-fit scheduling (fallback if OR-Tools is not installed)."""
    day_slots = slotify(24 * 60, slot_minutes)
    horizon_slots = slotify((mission_days + 1) * 24 * 60, slot_minutes)

    occupied, scheduled, _fixed_windows = preprocess_fixed_blocks(
        mission_days=mission_days,
        slot_minutes=slot_minutes,
        lanes=lanes,
        fixed_blocks=fixed_blocks,
    )

    def add_occupied(lane_list: List[str], start: int, dur: int) -> None:
        end = start + dur
        for lane in lane_list:
            occupied[lane].append((start, end))

    def is_free(lane_list: List[str], start: int, dur: int) -> bool:
        end = start + dur
        for lane in lane_list:
            for (s, e) in occupied[lane]:
                if not (end <= s or start >= e):
                    return False
        return True

    for lane in occupied:
        occupied[lane].sort()

    default_windows = build_default_windows()

    def candidate_windows_for(req: ActivityRequest) -> List[Tuple[int, int]]:
        windows = [TimeWindow(parse_hhmm(w["from"]), parse_hhmm(w["to"])) for w in req.windows] if req.windows else default_windows
        days = [req.day] if req.day is not None else list(range(mission_days))
        out: List[Tuple[int, int]] = []
        for d in days:
            for w in windows:
                out.append((d * day_slots + slotify(w.start_minute, slot_minutes),
                            d * day_slots + slotify(w.end_minute, slot_minutes)))
        return out

    for req in requests:
        lanes_for_req = participants_to_lanes(req.participants, lanes)
        dur_min = slotify(req.duration_min, slot_minutes)
        if dur_min <= 0:
            raise ValueError(f"Invalid duration for {req.name}")
        windows = candidate_windows_for(req)

        placed = False
        for (wstart, wend) in windows:
            t = wstart
            while t + dur_min <= wend and t + dur_min <= horizon_slots:
                if is_free(lanes_for_req, t, dur_min):
                    add_occupied(lanes_for_req, t, dur_min)
                    scheduled.append(ScheduledActivity(req, t, dur_min, lanes_for_req))
                    placed = True

                    if req.kind and req.kind.upper() == "EVA":
                        relax_min = req.relax_after_min or 60
                        relax_dur = slotify(relax_min, slot_minutes)
                        relax_start = t + dur_min
                        relax_req = ActivityRequest(
                            name="Relax time",
                            duration_min=relax_min,
                            participants=req.participants,
                            type="EVENT",
                            description="Auto-added relax time after EVA",
                        )
                        if not is_free(lanes_for_req, relax_start, relax_dur):
                            raise RuntimeError(f"Cannot place Relax time immediately after EVA {req.name}")
                        add_occupied(lanes_for_req, relax_start, relax_dur)
                        scheduled.append(ScheduledActivity(relax_req, relax_start, relax_dur, lanes_for_req))
                    break
                t += 1
            if placed:
                break
        if not placed:
            raise RuntimeError(f"Could not place activity {req.name} with greedy scheduler")

    scheduled.sort(key=lambda x: x.start_slot)
    return scheduled


def cpsat_schedule(
    *,
    mission_days: int,
    slot_minutes: int,
    lanes: List[CrewLane],
    fixed_blocks: List[RecurringBlock],
    requests: List[ActivityRequest],
    time_limit_s: int = 20,
) -> List[ScheduledActivity]:
    """Optimised schedule using OR-Tools CP-SAT."""
    from ortools.sat.python import cp_model  # type: ignore

    model = cp_model.CpModel()

    def new_int_var(lb: int, ub: int, name: str):
        fn = getattr(model, "new_int_var", None) or getattr(model, "NewIntVar")
        return fn(lb, ub, name)

    def new_bool_var(name: str):
        fn = getattr(model, "new_bool_var", None) or getattr(model, "NewBoolVar")
        return fn(name)

    def new_interval_var(start, size, end, name: str):
        fn = getattr(model, "new_interval_var", None) or getattr(model, "NewIntervalVar")
        return fn(start, size, end, name)

    def add_no_overlap(intervals: List[Any]):
        fn = getattr(model, "add_no_overlap", None) or getattr(model, "AddNoOverlap")
        return fn(intervals)

    def add_exactly_one(bools: List[Any]):
        fn = getattr(model, "add_exactly_one", None) or getattr(model, "AddExactlyOne")
        return fn(bools)

    def add(c):
        fn = getattr(model, "add", None) or getattr(model, "Add")
        return fn(c)

    def only_enforce_if(constraint: Any, lit: Any) -> Any:
        if hasattr(constraint, "OnlyEnforceIf"):
            constraint.OnlyEnforceIf(lit)
        else:
            constraint.only_enforce_if(lit)
        return constraint

    day_slots = slotify(24 * 60, slot_minutes)
    horizon_slots = slotify((mission_days + 1) * 24 * 60, slot_minutes)

    lane_intervals: Dict[str, List[Any]] = {l.lane: [] for l in lanes}
    _occupied, fixed_items_for_output, _fixed_windows = preprocess_fixed_blocks(
        mission_days=mission_days,
        slot_minutes=slot_minutes,
        lanes=lanes,
        fixed_blocks=fixed_blocks,
    )

    for fixed_item in fixed_items_for_output:
        s_slot = fixed_item.start_slot
        e_slot = fixed_item.start_slot + fixed_item.duration_slots
        interval = new_interval_var(s_slot, fixed_item.duration_slots, e_slot, f"fixed:{fixed_item.activity.name}:{s_slot}:{fixed_item.lanes[0]}")
        for ln in fixed_item.lanes:
            lane_intervals[ln].append(interval)

    default_windows = build_default_windows()

    def window_penalty(w: TimeWindow) -> int:
        return 10 if w.start_minute >= parse_hhmm("20:00") else 0

    scheduled_vars: List[Tuple[ActivityRequest, Any, Any, Any, List[str], List[Tuple[Any, TimeWindow]]]] = []

    for idx, req in enumerate(requests):
        lanes_for_req = participants_to_lanes(req.participants, lanes)

        dur_min_slots = slotify(req.duration_min, slot_minutes)
        dur_max_slots = slotify(req.duration_max, slot_minutes) if req.duration_max is not None else dur_min_slots
        dur_var = dur_min_slots if dur_min_slots == dur_max_slots else new_int_var(dur_min_slots, dur_max_slots, f"dur[{idx}]")

        start_var = new_int_var(0, horizon_slots, f"start[{idx}]")
        end_var = new_int_var(0, horizon_slots, f"end[{idx}]")
        interval = new_interval_var(start_var, dur_var, end_var, f"act:{idx}:{req.name}")

        windows = [TimeWindow(parse_hhmm(w["from"]), parse_hhmm(w["to"])) for w in req.windows] if req.windows else default_windows
        days = [req.day] if req.day is not None else list(range(mission_days))

        window_bools: List[Tuple[Any, TimeWindow]] = []
        for d in days:
            for w in windows:
                abs_ws = d * day_slots + slotify(w.start_minute, slot_minutes)
                abs_we = d * day_slots + slotify(w.end_minute, slot_minutes)
                b = new_bool_var(f"inwin[{idx}][{d}][{w.start_minute}-{w.end_minute}]")
                window_bools.append((b, w))
                only_enforce_if(add(start_var >= abs_ws), b)
                only_enforce_if(add(end_var <= abs_we), b)

        add_exactly_one([b for (b, _) in window_bools])

        for ln in lanes_for_req:
            lane_intervals[ln].append(interval)

        scheduled_vars.append((req, start_var, end_var, dur_var, lanes_for_req, window_bools))

    for ln, intervals in lane_intervals.items():
        add_no_overlap(intervals)

    relax_vars: List[Tuple[ActivityRequest, Any, int, List[str]]] = []
    for idx, (req, start_var, end_var, dur_var, lanes_for_req, window_bools) in enumerate(scheduled_vars):
        if req.kind and req.kind.upper() == "EVA":
            relax_min = req.relax_after_min or 60
            relax_slots = slotify(relax_min, slot_minutes)
            r_start = new_int_var(0, horizon_slots, f"relax_start[{idx}]")
            r_end = new_int_var(0, horizon_slots, f"relax_end[{idx}]")
            r_interval = new_interval_var(r_start, relax_slots, r_end, f"relax[{idx}]")
            add(r_start == end_var)
            add(r_end == end_var + relax_slots)
            for ln in lanes_for_req:
                lane_intervals[ln].append(r_interval)
            relax_req = ActivityRequest(
                name="Relax time",
                duration_min=relax_min,
                participants=req.participants,
                type="EVENT",
                description="Auto-added relax time after EVA",
            )
            relax_vars.append((relax_req, r_start, relax_slots, lanes_for_req))

    terms: List[Any] = []
    for (_req, start_var, _end_var, _dur_var, _lanes_for_req, window_bools) in scheduled_vars:
        for (b, w) in window_bools:
            pen = window_penalty(w)
            if pen:
                terms.append(pen * 1000 * b)
        terms.append(start_var)

    if hasattr(model, "Minimize"):
        model.Minimize(sum(terms))
    else:
        model.minimize(sum(terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(time_limit_s)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        raise RuntimeError(f"No feasible schedule found. Solver status={status}")

    out: List[ScheduledActivity] = []
    out.extend(fixed_items_for_output)

    for (req, start_var, _end_var, dur_var, lanes_for_req, _window_bools) in scheduled_vars:
        s = int(solver.Value(start_var))
        dur = int(dur_var) if isinstance(dur_var, int) else int(solver.Value(dur_var))
        out.append(ScheduledActivity(req, s, dur, lanes_for_req))

    for (req, r_start, r_dur, lanes_for_req) in relax_vars:
        s = int(solver.Value(r_start))
        out.append(ScheduledActivity(req, s, r_dur, lanes_for_req))

    out.sort(key=lambda x: x.start_slot)
    return out


def scheduled_to_yamcs_items(
    *,
    scheduled: List[ScheduledActivity],
    mission_start: datetime,
    timezone: str,
    slot_minutes: int,
    source: str = "rdb",
) -> List[Dict[str, Any]]:
    tzinfo = ensure_tzinfo(timezone)
    out: List[Dict[str, Any]] = []

    for sa in scheduled:
        start_dt = mission_start + timedelta(minutes=unslotify(sa.start_slot, slot_minutes))
        start_dt = start_dt.astimezone(tzinfo)
        duration_s = int(unslotify(sa.duration_slots, slot_minutes) * 60)

        for lane in sa.lanes:
            item: Dict[str, Any] = {
                "source": source,
                "id": str(uuid.uuid4()),
                "name": sa.activity.name,
                "type": sa.activity.type,
                "start": to_rfc3339(start_dt),
                "duration": duration_seconds_string(duration_s),
                "tags": [lane] + list(sa.activity.tags_extra),
                "description": sa.activity.description or "",
                "properties": {
                    "lane": str(lane),
                    "timezone": str(timezone),
                },
            }
            out.append(item)

    out.sort(key=lambda x: (x["start"], x["name"], x["properties"].get("lane", "")))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to mission_config.json")
    ap.add_argument("--out", required=True, help="Path to write Yamcs timeline-items JSON")
    args = ap.parse_args()

    cfg = json.load(open(args.config, "r", encoding="utf-8"))

    mission_cfg = cfg["mission"]
    tz = mission_cfg.get("timezone", "UTC")
    start_date = parse_date_yyyy_mm_dd(mission_cfg["start_date"])
    days = int(mission_cfg["days"])
    slot_minutes = int(cfg.get("options", {}).get("slot_minutes", 15))

    tzinfo = ensure_tzinfo(tz)
    mission_start = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=tzinfo)

    crew_cfg = cfg["crew"]["lanes"]
    lanes = [CrewLane(lane=c["lane"], role=c.get("role", "")) for c in crew_cfg]

    blocks_cfg = cfg.get("constraints", {}).get("recurring_blocks", [])
    fixed_blocks = [
        RecurringBlock(
            name=b["name"],
            start_hhmm=b["start"],
            end_hhmm=b["end"],
            participants=b.get("participants", ["ALL"]),
            days=b.get("days", "DAILY"),
            description=b.get("description", ""),
            priority=int(b.get("priority", 100)),
        )
        for b in blocks_cfg
    ]

    activities_cfg = cfg.get("activities", [])
    requests = [
        ActivityRequest(
            name=a["name"],
            kind=a.get("kind"),
            duration_min=int(a["duration_min"]),
            duration_max=int(a["duration_max"]) if "duration_max" in a else None,
            participants=a.get("participants", ["ALL"]),
            day=int(a["day"]) if "day" in a else None,
            windows=a.get("windows"),
            relax_after_min=int(a["relax_after_min"]) if "relax_after_min" in a else None,
            description=a.get("description", ""),
        )
        for a in activities_cfg
    ]

    use_cpsat = bool(cfg.get("options", {}).get("use_cpsat", True))
    time_limit_s = int(cfg.get("options", {}).get("time_limit_s", 20))
    source = cfg.get("yamcs", {}).get("source", "rdb")

    if use_cpsat:
        try:
            scheduled = cpsat_schedule(
                mission_days=days,
                slot_minutes=slot_minutes,
                lanes=lanes,
                fixed_blocks=fixed_blocks,
                requests=requests,
                time_limit_s=time_limit_s,
            )
        except Exception as e:
            print(f"[WARN] CP-SAT scheduling failed: {e}")
            print("[WARN] Falling back to greedy scheduling.")
            scheduled = greedy_schedule(
                mission_days=days,
                slot_minutes=slot_minutes,
                lanes=lanes,
                fixed_blocks=fixed_blocks,
                requests=requests,
            )
    else:
        scheduled = greedy_schedule(
            mission_days=days,
            slot_minutes=slot_minutes,
            lanes=lanes,
            fixed_blocks=fixed_blocks,
            requests=requests,
        )

    items = scheduled_to_yamcs_items(
        scheduled=scheduled,
        mission_start=mission_start,
        timezone=tz,
        slot_minutes=slot_minutes,
        source=source,
    )

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)

    print(f"Wrote {len(items)} Yamcs timeline items to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
