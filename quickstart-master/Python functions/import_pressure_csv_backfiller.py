import csv
import glob
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ---------------------------
# Configuration
# ---------------------------
YAMCS_URL = "http://localhost:8090"
INSTANCE = "adama"
PROCESSOR = "realtime"
AUTH = ("admin", "admin")

CSV_GLOB = r"C:\Users\39389\OneDrive\Desktop\TU Delft\Internship\ICEE.Space\quickstart-master\data\*.csv"
DELIMITER = ";"
ENCODING = "utf-8-sig"
BATCH_SIZE = 200
REQUEST_TIMEOUT = 20

# Historical CSV import into a realtime processor is safest when everything is
# merged and sorted globally before any request is sent.
GLOBAL_SORT_ACROSS_FILES = True

# Safety check: abort before sending if we still detect time going backwards.
ABORT_ON_NON_MONOTONIC_SEQUENCE = True

# Historical archive rebuild
AUTO_REBUILD_ARCHIVE = True
REBUILD_ONLY_IF_SAMPLES_SENT = True

PARAM_MAP: Dict[str, str] = {
    "Pressure Inside": "/ADAMA/pressure_inside",
    "Pressure Outside": "/ADAMA/pressure_outside",
    "Pressure Left Side": "/ADAMA/pressure_left_side",
    "Pressure Right Side": "/ADAMA/pressure_right_side",
}


# ---------------------------
# Data structures
# ---------------------------
@dataclass
class Sample:
    generation_dt: datetime
    generation_time: str
    parameter_name: str
    float_value: float
    source_file: str
    source_row: int


@dataclass
class FileStats:
    file_path: str
    rows_seen: int = 0
    rows_with_valid_time: int = 0
    samples_built: int = 0
    rows_skipped_bad_time: int = 0
    values_skipped_bad_float: int = 0
    values_skipped_empty: int = 0


# ---------------------------
# Parsing helpers
# ---------------------------
def to_fake_utc_datetime(date_str: str, time_str: str) -> datetime:
    candidate = f"{date_str.strip()} {time_str.strip()}"

    formats = [
        "%d/%m/%Y %H:%M:%S",  # 17/10/2025 00:00:33
        "%Y-%m-%d %H:%M:%S",  # 2025-10-10 00:00:09
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(candidate, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    raise ValueError(
        f"unsupported datetime format: '{candidate}'"
    )


def to_iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def parse_float(raw_value: str) -> Optional[float]:
    text = raw_value.strip()
    if text == "":
        return None

    normalized = text.replace(",", ".")
    value = float(normalized)
    if not math.isfinite(value):
        raise ValueError(f"non-finite float: {raw_value}")
    return value


# ---------------------------
# Yamcs posting helpers
# ---------------------------
def build_request(sample: Sample) -> dict:
    return {
        "id": {"name": sample.parameter_name},
        "value": {
            "type": "FLOAT",
            "floatValue": sample.float_value,
        },
        "generationTime": sample.generation_time,
    }


def post_batch(session: requests.Session, batch: List[dict]) -> int:
    if not batch:
        return 0

    url = f"{YAMCS_URL}/api/processors/{INSTANCE}/{PROCESSOR}/parameters:batchSet"
    response = session.post(url, json={"request": batch}, timeout=REQUEST_TIMEOUT)

    if response.status_code not in (200, 204):
        print(f"ERROR batch: {response.status_code} {response.text}")
        return 0

    return len(batch)


def rebuild_archive_range(
    session: requests.Session, start_dt: datetime, stop_dt: datetime
) -> bool:
    url = f"{YAMCS_URL}/api/archive/{INSTANCE}/parameterArchive:rebuild"

    # Make the stop inclusive enough for the last imported second.
    stop_inclusive = stop_dt + timedelta(seconds=1)

    payload = {
        "start": to_iso_z(start_dt),
        "stop": to_iso_z(stop_inclusive),
    }

    print("Triggering Parameter Archive rebuild")
    print(f" - start: {payload['start']}")
    print(f" - stop:  {payload['stop']}")

    response = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)

    if response.status_code not in (200, 204):
        print(f"ERROR rebuild: {response.status_code} {response.text}")
        return False

    print("Archive rebuild request accepted.")
    return True


# ---------------------------
# CSV parsing
# ---------------------------
def parse_file(csv_path: str) -> Tuple[List[Sample], FileStats]:
    stats = FileStats(file_path=csv_path)
    samples: List[Sample] = []

    with open(csv_path, newline="", encoding=ENCODING) as handle:
        reader = csv.DictReader(handle, delimiter=DELIMITER)

        for row_index, row in enumerate(reader, start=2):
            stats.rows_seen += 1

            date_raw = (row.get("Date") or "").strip()
            time_raw = (row.get("Time") or "").strip()

            if not date_raw or not time_raw:
                stats.rows_skipped_bad_time += 1
                continue

            if date_raw.lower() == "date" or time_raw.lower() == "time":
                stats.rows_skipped_bad_time += 1
                continue

            try:
                generation_dt = to_fake_utc_datetime(date_raw, time_raw)
            except Exception as exc:
                stats.rows_skipped_bad_time += 1
                print(f"Skipping row {row_index} in {csv_path}: bad date/time ({exc})")
                continue

            stats.rows_with_valid_time += 1
            generation_time = to_iso_z(generation_dt)

            for csv_col, yamcs_param in PARAM_MAP.items():
                raw_value = row.get(csv_col)
                if raw_value is None:
                    stats.values_skipped_empty += 1
                    continue

                try:
                    parsed = parse_float(raw_value)
                except ValueError:
                    stats.values_skipped_bad_float += 1
                    print(
                        f"Skipping row {row_index}, column '{csv_col}' in {csv_path}: bad float '{raw_value}'"
                    )
                    continue

                if parsed is None:
                    stats.values_skipped_empty += 1
                    continue

                samples.append(
                    Sample(
                        generation_dt=generation_dt,
                        generation_time=generation_time,
                        parameter_name=yamcs_param,
                        float_value=parsed,
                        source_file=csv_path,
                        source_row=row_index,
                    )
                )
                stats.samples_built += 1

    return samples, stats


# ---------------------------
# Ordering diagnostics
# ---------------------------
def sort_samples(samples: List[Sample]) -> None:
    samples.sort(key=lambda s: (s.generation_dt, s.source_file, s.source_row, s.parameter_name))


def find_first_backwards_step(samples: List[Sample]) -> Optional[Tuple[int, Sample, Sample]]:
    if not samples:
        return None

    prev = samples[0]
    for idx in range(1, len(samples)):
        cur = samples[idx]
        if cur.generation_dt < prev.generation_dt:
            return idx, prev, cur
        prev = cur
    return None


def print_ordering_report(samples: List[Sample]) -> None:
    if not samples:
        print("No samples prepared.")
        return

    print("Ordering check")
    print(f" - first sample time: {samples[0].generation_time}")
    print(f" - last sample time:  {samples[-1].generation_time}")
    print(f" - total samples:     {len(samples)}")

    violation = find_first_backwards_step(samples)
    if violation is None:
        print(" - monotonic check:   OK")
    else:
        idx, prev, cur = violation
        print(" - monotonic check:   FAILED")
        print(f"   previous: {prev.generation_time} | {Path(prev.source_file).name}:{prev.source_row}")
        print(f"   current:  {cur.generation_time} | {Path(cur.source_file).name}:{cur.source_row}")
        print(f"   position: {idx}")


# ---------------------------
# Sending
# ---------------------------
def send_samples(session: requests.Session, samples: List[Sample]) -> int:
    sent = 0
    batch: List[dict] = []
    last_sent_dt: Optional[datetime] = None

    for sample in samples:
        if last_sent_dt is not None and sample.generation_dt < last_sent_dt:
            raise RuntimeError(
                "Internal ordering error: sample sequence moved backwards while sending: "
                f"{sample.generation_time} after {to_iso_z(last_sent_dt)}"
            )

        batch.append(build_request(sample))
        last_sent_dt = sample.generation_dt

        if len(batch) >= BATCH_SIZE:
            sent += post_batch(session, batch)
            batch = []

    sent += post_batch(session, batch)
    return sent


# ---------------------------
# Reporting helpers
# ---------------------------
def print_file_stats(stats: FileStats) -> None:
    print(
        "Finished"
        f" {stats.file_path}"
        f" | rows seen: {stats.rows_seen}"
        f" | valid timestamps: {stats.rows_with_valid_time}"
        f" | samples built: {stats.samples_built}"
        f" | bad time rows: {stats.rows_skipped_bad_time}"
        f" | bad floats: {stats.values_skipped_bad_float}"
        f" | empty values: {stats.values_skipped_empty}"
    )


# ---------------------------
# Main
# ---------------------------
def main() -> None:
    files = sorted(glob.glob(CSV_GLOB))
    if not files:
        print(f"No CSV files found for: {CSV_GLOB}")
        return

    print(f"Found {len(files)} CSV file(s)")
    for file_path in files:
        print(f" - {Path(file_path).name}")

    all_samples: List[Sample] = []
    all_stats: List[FileStats] = []

    for csv_path in files:
        print(f"Parsing {csv_path}")
        samples, stats = parse_file(csv_path)
        all_samples.extend(samples)
        all_stats.append(stats)

    if GLOBAL_SORT_ACROSS_FILES:
        sort_samples(all_samples)

    print_ordering_report(all_samples)

    if ABORT_ON_NON_MONOTONIC_SEQUENCE:
        violation = find_first_backwards_step(all_samples)
        if violation is not None:
            print("Aborting before sending because the final sample list is not monotonic.")
            return

    total_sent = 0
    rebuild_ok = None

    with requests.Session() as session:
        session.auth = AUTH
        total_sent = send_samples(session, all_samples)

        should_rebuild = AUTO_REBUILD_ARCHIVE
        if REBUILD_ONLY_IF_SAMPLES_SENT and total_sent <= 0:
            should_rebuild = False

        if should_rebuild and all_samples:
            rebuild_ok = rebuild_archive_range(
                session,
                start_dt=all_samples[0].generation_dt,
                stop_dt=all_samples[-1].generation_dt,
            )

    for stats in all_stats:
        print_file_stats(stats)

    print("-" * 80)
    print(f"Files processed:  {len(files)}")
    print(f"Samples prepared: {len(all_samples)}")
    print(f"Samples sent:     {total_sent}")

    if AUTO_REBUILD_ARCHIVE:
        if rebuild_ok is True:
            print("Archive rebuild:  requested successfully")
        elif rebuild_ok is False:
            print("Archive rebuild:  request failed")
        else:
            print("Archive rebuild:  skipped")

    print("Done.")


if __name__ == "__main__":
    main()
