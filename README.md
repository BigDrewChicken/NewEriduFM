<p align="center">
  <img src="assets/header.svg" alt="NEW ERIDU FM: Signal Search Big Data Pipeline" width="100%">
</p>

<p align="center">
  A streaming data pipeline that simulates the events of Zenless Zone Zero's "Signal Search" system would generate
  (pulls and top-ups from 1,200 synthetic players), sends them through Apache Kafka into MongoDB,
  and shows them on a live retro radio-station dashboard.
</p>

<p align="center">

![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Storage-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containers-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-Dashboard-000000?style=for-the-badge&logo=flask&logoColor=white)

![HTML5](https://img.shields.io/badge/HTML5-Frontend-E34F26?style=for-the-badge&logo=html5&logoColor=white)
![CSS3](https://img.shields.io/badge/CSS3-Styling-1572B6?style=for-the-badge&logo=css3&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-Live%20UI-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)
![Windows](https://img.shields.io/badge/Windows-10%2F11-0078D4?style=for-the-badge&logo=windows&logoColor=white)
![Git](https://img.shields.io/badge/Git-Version%20Control-F05032?style=for-the-badge&logo=git&logoColor=white)
![VS Code](https://img.shields.io/badge/VS%20Code-Editor-007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white)

</p>

---

## Table of Contents

- [About the Project](#about-the-project)
- [Features](#features)
- [How It Works](#how-it-works)
- [The Three Vs of Big Data](#the-three-vs-of-big-data)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Get the code](#1-get-the-code)
  - [2. Create the virtual environment](#2-create-the-virtual-environment)
  - [3. Activate the virtual environment](#3-activate-the-virtual-environment)
  - [4. Install the dependencies](#4-install-the-dependencies)
  - [5. Configure the environment file](#5-configure-the-environment-file)
  - [6. Start Kafka and MongoDB](#6-start-kafka-and-mongodb)
  - [7. Run the pipeline](#7-run-the-pipeline)
  - [8. Open the dashboard](#8-open-the-dashboard)
  - [9. Stop everything](#9-stop-everything)
- [Checking the Data](#checking-the-data)
- [Understanding the Pipeline](#understanding-the-pipeline)
- [Understanding the Virtual Environment](#understanding-the-virtual-environment)
- [Event Schemas](#event-schemas)
- [Tuning the Producer](#tuning-the-producer)
- [Optional: MongoDB Atlas](#optional-mongodb-atlas)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)
- [License](#license)
- [Project Creators](#project-creators)

---

## About the Project

**NEW ERIDU FM** is a big data demo, not a game. It does not model combat, characters or any real game system. It simulates **the data a Signal Search system would generate**: a high-rate stream of pull and top-up events, up to 10,000 events per second, from 1,200 synthetic players.

The goal is to show a real streaming pipeline on a single Windows PC and to demonstrate the **three Vs of Big Data** (volume, velocity and variety) with numbers you can watch change live.

> Unofficial fan and educational project. Not affiliated with or endorsed by HoYoverse. *Zenless Zone Zero* and related names are trademarks of their owners. All data is synthetic, and prices, drop rates and item names are **illustrative**, not official numbers.

## Features

- **High-rate event producer.** Ramps from 1,000 to 10,000 events/s with a single command, with a dry-run mode for speed tests.
- **Two event types in one stream.** Signal Search pulls (with pity, banners and S/A ranks) and PHP top-ups (GCash, Maya, Card, GrabPay).
- **Heavy-tailed players.** A few "whales" dominate spending, so the top-spenders chart looks realistic.
- **Kafka as the backbone.** A 6-partition topic with two independent consumer groups reading the same events.
- **MongoDB storage.** Every raw event is stored locally, with TTL expiry to keep disk use bounded.
- **Live dashboard.** The "NEW ERIDU FM" page shows events/s, spinning reels, top spenders, S-rank drops and consumer lag.
- **Optional cloud rollups.** MongoDB Atlas can receive per-second rollups and a throttled sample.
- **Demo mode.** The dashboard can run on a fake in-process stream, with no Docker needed.

## How It Works

```
producer.py --(JSON events, up to 10k/s)--> Kafka (Docker, topic zzz.events, 6 partitions)
                                              |-- dashboard.py  (consumer group "zzz-dashboard") -> http://127.0.0.1:5000
                                              |-- sink.py       (consumer group "zzz-mongo-sink")
                                                    |-- local MongoDB (Docker): ALL raw events, TTL-expiring
                                                    |-- Atlas M0 (optional cloud): rollups, top spenders, sampled events
```

1. `producer.py` generates synthetic pull and top-up events and publishes them to the Kafka topic `zzz.events`.
2. Kafka stores the events for 15 minutes. It never writes to MongoDB by itself.
3. `dashboard.py` is one consumer. It reads the stream and drives the live web page.
4. `sink.py` is a second, independent consumer. It reads the same stream and writes the events to MongoDB.

**Why is raw data stored locally and not in Atlas?** The free Atlas M0 tier allows about 100 operations per second and 512 MB. At 10,000 events/s (about 4 MB/s) it would fill in minutes. So the full-rate stream goes to local MongoDB, and Atlas, if you use it, only receives about 32 operations per second of rollups and samples. Keeping the hot path local and sending aggregates to the cloud is a common real-world pattern.

## The Three Vs of Big Data

| V | Where you see it |
|---|---|
| **Volume** | The total events and MB counters. Ten minutes at 10,000 events/s is about 6 million events, roughly 2.6 GB of JSON. |
| **Velocity** | The live events/s graph, the spinning reels (faster under load) and **consumer lag**. Lag near 0 means the consumers keep up. |
| **Variety** | Two event types with different schemas in one topic, a nested list field (`batch_ranks`), and flexible MongoDB documents with no fixed table schema. |

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Message broker | Apache Kafka (single broker, KRaft mode, Docker) |
| Kafka client | confluent-kafka |
| Database | MongoDB 7 (local, Docker) and optional MongoDB Atlas M0 |
| Database driver | PyMongo |
| Web framework | Flask |
| Frontend | HTML5, CSS3, JavaScript |
| Containers | Docker Desktop (WSL 2 backend) |
| Configuration | python-dotenv |

## Project Structure

```
New-Eridu-FM/
├── assets/
│   └── banner.gif              # Optional header image (your own)
├── data/                       # Input files: items list and 1,200 synthetic users
├── templates/
│   └── dashboard.html          # NEW ERIDU FM live dashboard page
├── common.py                   # Shared event-generation helpers
├── producer.py                 # Generates events and publishes them to Kafka
├── sink.py                     # Kafka -> MongoDB consumer (local, plus optional Atlas)
├── dashboard.py                # Live consumer and Flask web server
├── docker-compose.yml          # Kafka (KRaft, 15-min retention) and MongoDB with memory limits
├── requirements.txt            # Python dependencies
├── .env.example                # Copy to .env (never commit .env)
├── .gitignore
├── LICENSE
└── README.md
```

> **Note:** This tree follows the planned layout. Adjust it to match your repository if a file lives somewhere else.

---

## Getting Started

Follow these steps in order. Everything runs on Windows, in PowerShell.

### Prerequisites

| Tool | Version | Check with |
|---|---|---|
| Windows | 10 or 11, with virtualization enabled | Task Manager > Performance > CPU > "Virtualization: Enabled" |
| Docker Desktop | WSL 2 backend | `docker version` |
| Python | 3.11 or newer | `python --version` |
| Git (optional) | Any recent version | `git --version` |

You also need about 4 GB of RAM free for Docker.

> **Windows tip:** When installing Python, tick **"Add python.exe to PATH"** on the first installer screen. If `python` is not recognized later, this is almost always why. Docker can be installed with `winget install -e --id Docker.DockerDesktop`.

> **Recommended:** Cap Docker's resources so your PC stays usable. Create `C:\Users\<you>\.wslconfig`:
>
> ```
> [wsl2]
> memory=4GB
> processors=4
> swap=2GB
> ```
>
> Then run `wsl --shutdown` and restart Docker Desktop. (On an 8 GB RAM PC, use `memory=3GB`.)

### 1. Get the code

Clone the repository (or download it as a ZIP and extract it):

```
git clone <your-repository-url>
cd <repository-folder>
```

### 2. Create the virtual environment

A virtual environment (`.venv`) is a private folder that holds this project's Python packages. From inside the project folder, run:

```
python -m venv .venv
```

You only need to do this **once per computer**.

### 3. Activate the virtual environment

You must activate the environment **every time you open a new terminal** for this project, including each of the three windows used in step 7.

**PowerShell or the VS Code terminal:**

```
.\.venv\Scripts\Activate.ps1
```

**Command Prompt:**

```
.venv\Scripts\activate.bat
```

When it works, your prompt starts with `(.venv)`.

> **PowerShell says "running scripts is disabled"?** Run this once, then try again:
>
> ```
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```

### 4. Install the dependencies

With `(.venv)` showing in your prompt:

```
pip install -r requirements.txt
```

### 5. Configure the environment file

```
copy .env.example .env
```

| Variable | Meaning |
|---|---|
| `MONGO_ATLAS_URI` | Leave **empty** for local-only mode (recommended). Paste your Atlas connection string to also send rollups to the cloud. |
| `LOCAL_TTL_MIN` | Minutes before local MongoDB deletes old events (default `20`). |

Never commit `.env`. It is listed in `.gitignore` because it can hold your Atlas password.

### 6. Start Kafka and MongoDB

Open Docker Desktop and wait for **"Engine running"**, then:

```
docker compose up -d
docker compose ps
```

`zzz-kafka` and `zzz-mongo` should both say **running**. The first run downloads the images, which takes a few minutes.

### 7. Run the pipeline

**Start the readers first, then the producer.** The dashboard only shows events that arrive after it started. Open three PowerShell windows in the project folder, with the virtual environment activated in each.

| Window | Command | Success looks like |
|---|---|---|
| 1 | `python dashboard.py` | `Dashboard: http://127.0.0.1:5000` (shows NO SIGNAL until data arrives) |
| 2 | `python sink.py` | `Sink running.` |
| 3 | `python producer.py --rate 10000 --start-rate 1000 --ramp 60 --duration 300` | A line each second: `target=... actual=... MB/s` |

A 5-minute run is roughly 2.7 million events, about 1.2 GB of JSON. That is plenty for a demo.

**What you should see:**

- **Producer:** `actual` follows `target` as it ramps from 1,000 to 10,000 events/s.
- **Dashboard:** ON AIR, events/s climbing, reels spinning faster, top spenders and S-rank drops updating.
- **Sink:** `consumed=... local_inserted=...` every few seconds, with both numbers rising together.
- **Consumer lag** near 0 means the readers keep up. If it keeps climbing, that is a real bottleneck finding worth discussing.

**Safe ramp plan:** run `--rate 1000` for a minute, then `--rate 5000`, then `--rate 10000`, and watch Task Manager (CPU under about 85%).

### 8. Open the dashboard

Go to:

```
http://127.0.0.1:5000
```

`http://localhost:5000` works too. If your terminal shows a different port, use that one.

> No Docker yet? `python dashboard.py --demo` shows the dashboard on a fake in-process stream so you can preview the design.

### 9. Stop everything

1. Press **Ctrl + C** in the producer window (or let `--duration` finish), then wait about 10 seconds for the readers to drain.
2. Press **Ctrl + C** in the sink and dashboard windows.
3. Shut down the containers:

```
docker compose down        # stop, keep the MongoDB data
docker compose down -v     # stop and wipe all data (fresh start)
```

To leave the virtual environment, type `deactivate`.

---

## Checking the Data

The producer writes to **Kafka only**. MongoDB receives data only while `sink.py` is running.

### MongoDB

The data lives in a Docker volume, not in a project file, so you query it through MongoDB. Open the shell, then paste the commands one at a time (without any leading `|` characters):

```
docker exec -it zzz-mongo mongosh
```

```js
use zzz
db.events.countDocuments()
db.events.findOne()
db.events.aggregate([{ $group: { _id: "$event_type", n: { $sum: 1 } } }])
db.events.aggregate([{ $match: { event_type: "topup" } }, { $group: { _id: "$username", php: { $sum: "$price_php" } } }, { $sort: { php: -1 } }, { $limit: 5 }])
```

The last query is the top-spenders ranking. It should match the dashboard when both read the same events. Type `exit` to leave the shell.

One-liner from PowerShell:

```
docker exec -it zzz-mongo mongosh --quiet --eval "db.getSiblingDB('zzz').events.countDocuments()"
```

**GUI options:** connect **MongoDB Compass** or the **MongoDB for VS Code** extension to `mongodb://localhost:27017`, then browse `zzz > events`.

> Local MongoDB deletes events older than `LOCAL_TTL_MIN` (20 minutes by default), so take screenshots during or right after a run.

### Kafka

```
# the topic layout
docker exec -it zzz-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic zzz.events

# how far behind the MongoDB sink is (see the LAG column)
docker exec -it zzz-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group zzz-mongo-sink

# peek at 5 raw events
docker exec -it zzz-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic zzz.events --from-beginning --max-messages 5
```

### Atlas (only if configured)

In Atlas, open **Browse Collections** and look for `zzz.rollups_1s` (a new document every second), `zzz.sampled_events` and `zzz.top_spenders`.

## Understanding the Pipeline

**Why can the dashboard show data while MongoDB is empty?** The dashboard reads straight from Kafka. MongoDB is filled only by `sink.py`. If the sink is not running, or it crashed, Kafka and the dashboard still work and MongoDB stays empty. The events are not lost. Kafka keeps them for 15 minutes.

**Why might the dashboard and MongoDB totals differ?**

- The dashboard counts only events that arrive after it starts. The sink resumes from where it last stopped.
- MongoDB deletes old events through its TTL, so its totals shrink over time.
- A lagging sink has not caught up with what the dashboard already showed.

For matching totals, run a clean reset (`docker compose down -v`, then `docker compose up -d`) and start the dashboard and sink **before** the producer.

## Understanding the Virtual Environment

**What is `.venv`?** A folder containing its own copy of Python plus only the packages this project needs (Flask, PyMongo, confluent-kafka and so on). When it is *activated*, the `python` and `pip` commands use this private copy instead of the system-wide one.

**Why use one?**

- Different projects can use different package versions without breaking each other.
- Anyone who clones the repository can recreate the same setup.
- Your global Python stays clean.

**What is `requirements.txt`?** A plain list of the packages the project depends on. `pip install -r requirements.txt` installs them all in one go. After adding a package, refresh the list with `pip freeze > requirements.txt`.

**Why is `.venv` not in the repository?** It is large and specific to each computer, so it is in `.gitignore`. Each person creates their own with steps 2 to 4.

**Environment variables.** Settings and secrets live in `.env`, which is also in `.gitignore`. Only `.env.example` (with no real secrets) is committed.

**Common mistake:** running a script *without* `(.venv)` active. If you get `ModuleNotFoundError: No module named 'confluent_kafka'`, your environment is not activated, so go back to step 3.

## Event Schemas

**Pull** (one event per pull, sent in batches of 1 or 10 that share a `batch_id`):

```
event_type, ts, username, id, banner, pull_number, pull_id, batch_id, batch_size,
item_id, item_name, item_type, rank (S|A), pity_count, is_featured,
polychrome_spent, batch_ranks
```

**Top-up:**

```
event_type, event_id, ts, username, id, pack, polychrome, price_php,
payment (GCash | Maya | Card | GrabPay)
```

`ts` is **epoch milliseconds** (a number).

## Tuning the Producer

| Option | Meaning |
|---|---|
| `--rate N` | Target events per second (default 1000) |
| `--start-rate N` | Events per second at the start of the ramp |
| `--ramp N` | Seconds to ramp from `--start-rate` to `--rate` |
| `--duration N` | Seconds to run (0 means until Ctrl + C) |
| `--dry-run` | Generate and serialize only, with no Kafka (speed test) |
| `--sample N` | Print N sample events at start (default 3) |
| `--seed N` | Random seed for repeatable data |

If `actual` stays below `target`, your CPU is the limit. Use `--rate 5000`, or run two producers at `--rate 5000` each in two windows. To test the generator speed alone:

```
python producer.py --dry-run --rate 20000
```

## Optional: MongoDB Atlas

1. In [cloud.mongodb.com](https://cloud.mongodb.com), create a free **M0** cluster. Pick the region closest to the Philippines (for example Singapore).
2. **Database Access:** add a user with a username and password (use letters and digits, or URL-encode symbols). Role: *Read and write to any database*.
3. **Network Access:** add your current IP address. Re-add it if your IP changes (for example on a new Wi-Fi network).
4. **Database > Connect > Drivers > Python:** copy the `mongodb+srv://...` string, replace `<password>`, and paste it into `.env` as `MONGO_ATLAS_URI=...`.
5. Restart `sink.py`. It should print `Atlas connected.`

The collections are created automatically. Atlas receives about 32 operations per second, with TTL indexes (rollups 24 hours, samples 1 hour) so storage stays small.

## Troubleshooting

| Problem | Likely cause and fix |
|---|---|
| `python` is not recognized | Python is not on your PATH. Reinstall it and tick "Add to PATH", or try `py` instead of `python`. |
| `ModuleNotFoundError: No module named ...` | The virtual environment is not activated, or the dependencies are not installed. Redo steps 3 and 4. |
| PowerShell blocks `Activate.ps1` | Run `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`, then activate again. |
| `docker compose` cannot connect to the engine | Open Docker Desktop and wait for "Engine running". |
| `Cannot reach Kafka at localhost:9092` | Docker Desktop is not running, or Kafka is restarting. Check `docker compose ps` and `docker logs zzz-kafka` (usually it needs more memory in `.wslconfig`). |
| Dashboard stuck on NO SIGNAL | It was started after the producer, or the producer is not running. Start it first, or use `--from-start` to replay retained messages. |
| MongoDB count is 0 but the dashboard shows data | `sink.py` is not running or crashed. Check the sink window and the consumer lag command above. |
| `sink.py` crashes with a `TypeError` on `ts` | Old messages in a different format are still in Kafka. Reset with `docker compose down -v`, then `docker compose up -d`. |
| `consumed=0` in the sink | The producer is not running, or Kafka is not reachable. |
| `mongosh` says `use` or `db` is not recognized | You typed MongoDB commands in PowerShell. Open the shell first with `docker exec -it zzz-mongo mongosh`. |
| PC is lagging | Stop everything and rerun at `--rate 3000`. The rate flag is the safe dial. |
| Atlas errors | Wrong password, IP not allowlisted, or a special character in the password (URL-encode it). |
| Atlas "storage quota" errors | Drop the collections in Atlas, or lower the sample rate in `sink.py`. |
| Disk usage growing | Lower `LOCAL_TTL_MIN`, or run `docker compose down -v` when you are done. |

## Limitations

This is one Kafka broker on one PC, with synthetic data and illustrative rates and prices. The aim is to demonstrate the pipeline and the three Vs, not to benchmark a production cluster.

## License

This project is distributed under the terms of the license in the [LICENSE](LICENSE) file.

---

## Project Creators

[![Bulaong, Ross Andrew](https://img.shields.io/badge/Bulaong%2C%20Ross%20Andrew-FFD400?style=for-the-badge&logo=github&logoColor=black)](https://github.com/BigDrewChicken)

<!-- Add teammates the same way:
[![Surname, Given Name](https://img.shields.io/badge/Surname%2C%20Given%20Name-FFD400?style=for-the-badge&logo=github&logoColor=black)](https://github.com/their-username)
-->
