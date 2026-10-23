# yamcs-analogue-mission-template

Tools for planning and monitoring analogue astronaut missions with [Yamcs](https://yamcs.org), developed during an internship at ICEE.Space (February–April 2026).

The repository contains:

- a Yamcs server project with two instances, `adama` and `general`;
- a timeline generator that turns a mission configuration (JSON) into Yamcs timeline items, placing flexible activities automatically with a constraint solver;
- scripts to import timelines and historical sensor data into Yamcs;
- the data of the ADAMA mission (Adelaide, October 2025).

The tag `internship-final` marks the state of the repository at the end of the internship. Later commits contain changes made during the revision of the internship report.

## Repository structure

| Path | Content |
|---|---|
| `ADAMA_to_YAMCS.xlsx` | Original ADAMA master schedule |
| `quickstart-master/` | Yamcs server project (Maven) |
| `quickstart-master/src/main/yamcs/etc/` | Server and instance configuration (`yamcs.yaml`, `yamcs.adama.yaml`, `yamcs.general.yaml`, `processor.yaml`) |
| `quickstart-master/src/main/yamcs/mdb/` | Mission databases (`adama.xml`, `general.xml`, `april_mission.xml`) |
| `quickstart-master/data/` | ADAMA pressure logger data (CSV) |
| `quickstart-master/Python functions/` | Python scripts, mission configurations and timeline files |

Main Python files:

| File | Purpose |
|---|---|
| `mission_scheduler.py` | Generates Yamcs timeline items from a mission configuration |
| `import_timeline.py` | Imports timeline items into a Yamcs instance |
| `import_pressure_csv_backfiller.py` | Imports the ADAMA pressure CSV files as Yamcs parameters |
| `verify_adama_migration.py` | Compares `adama_timeline_items.json` with the original spreadsheet |
| `april_mission_config.json` | Configuration of the 24-hour simulation of April 2026 (fixed activities only) |
| `mission_48h_flexible_config.json` | 48-hour configuration with flexible activities (scheduling demonstration) |
| `mission_config_sample.json` | Small example configuration |

## Prerequisites

- Java 17+
- Linux x64/aarch64, macOS x64/aarch64, or Windows x64
- A copy of Maven (or the Maven wrapper included in `quickstart-master`)
- Python 3.9+ with the packages in `requirements.txt`:

```
python -m pip install -r requirements.txt
```

`ortools` is needed for optimised scheduling; without it, the scheduler falls back to a simple greedy method. `tzdata` is needed on Windows only.

## Running Yamcs

From `quickstart-master`:

```
mvnw.cmd yamcs:run        # Windows
./mvnw yamcs:run          # Linux / macOS / WSL
```

The web interface is available at <http://localhost:8090>.

## Generating a timeline

From `quickstart-master/Python functions`:

```
python mission_scheduler.py --config mission_48h_flexible_config.json --out timeline_items.json
```

The script reports the solver status, the number of flexible activities placed and the objective value. To compare with the greedy method:

```
python mission_scheduler.py --config mission_48h_flexible_config.json --out greedy_items.json --solver greedy
```

## Importing a timeline into Yamcs

Set the following values at the top of `import_timeline.py`, then run `python import_timeline.py` while Yamcs is running:

```python
YAMCS_URL = "http://localhost:8090"
INSTANCE = "general"
USERNAME = "<YAMCS_USERNAME>"
PASSWORD = "<YAMCS_PASSWORD>"
INPUT_FILE = "timeline_items.json"
```

In the Yamcs web interface, open the Timeline page of the instance, create one item band per lane (for example COM, ENG, SCI and MCC) that selects items by the lane tag, and save a view with these bands. Items carry their own colours, so the band colours do not need to be set.

## Importing the ADAMA pressure data

Set `AUTH` and `CSV_GLOB` at the top of `import_pressure_csv_backfiller.py`, then run it while Yamcs is running. The data are written to the `adama` instance and the Parameter Archive is rebuilt for the imported time range.

## Verifying the ADAMA migration

```
python verify_adama_migration.py ../../ADAMA_to_YAMCS.xlsx adama_timeline_items.json
```

## Mission configuration

A configuration file has the following sections:

| Section | Content |
|---|---|
| `mission` | Name, start date, number of days, time zone |
| `crew.lanes` | Lanes (crew members or teams) and their roles |
| `constraints.recurring_blocks` | Fixed activities: `name`, `start`, `end`, `participants`, `days` (`DAILY` or `DAYS:0,1`), `priority`, `kind` |
| `activities` | Flexible activities: `name`, `duration_min`, optional `duration_max`, `participants`, `day`, `windows`, `kind`, optional `after` |
| `styles.kinds` | Colours per activity kind (`item_background_color`, `item_border_color`) |
| `options` | `slot_minutes`, `use_cpsat`, `time_limit_s` |

Notes:

- When fixed blocks overlap on a lane, the block with the higher `priority` is kept and the other is cut around it; for equal priorities, the block listed first wins.
- `after` makes an activity start only after the named activity has ended (for example `"after": "IVA 1"`).
- An activity with `"kind": "EVA"` is followed automatically by a rest period (60 minutes by default, or `relax_after_min`).
- Mission days are numbered from 0.

## Known limitations

- **Time zones:** the timelines of this project store local clock times as if they were UTC. Activities therefore appear at their planned clock times on a UTC time axis, but the current-time indicator of Yamcs does not match local time.
- `april_mission.xml` is not loaded by any instance in the current configuration.
- The live bridge between Home Assistant and Yamcs was designed but not implemented.
