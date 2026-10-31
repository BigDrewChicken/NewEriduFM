"""Kafka -> MongoDB sink.
  * LOCAL Mongo (Docker): every raw event, bulk-inserted (handles 10k/s), auto-expires via TTL.
  * ATLAS (free M0, 100 ops/s cap): per-second rollups + top spenders + a throttled sample of raw events.
python sink.py"""

import json, os, queue, threading, time
from collections import Counter
from datetime import datetime, timezone
from confluent_kafka import Consumer
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from common import BOOTSTRAP, TOPIC

LOCAL_URI = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017")
ATLAS_URI = os.getenv("MONGO_ATLAS_URI", "").strip()
TTL_MIN = int(os.getenv("LOCAL_TTL_MIN", "20"))
SAMPLE_EVERY, ATLAS_SAMPLE_CAP = 1000, 30          # 1-in-1000 events, max 30 sample docs per second
now = lambda: datetime.now(timezone.utc)

local = MongoClient(LOCAL_URI)["zzz"]["events"]
local.create_index([("ts_dt", ASCENDING)], expireAfterSeconds=TTL_MIN * 60)

atlas_q = queue.Queue(maxsize=500)
def atlas_writer():
    db = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=8000)["zzz"]
    db.rollups_1s.create_index("ts_dt", expireAfterSeconds=86400)
    db.sampled_events.create_index("ts_dt", expireAfterSeconds=3600)
    print("Atlas connected.")
    while True:
        kind, payload = atlas_q.get()
        try:
            if kind == "rollup": db.rollups_1s.insert_one(payload)
            elif kind == "sample": db.sampled_events.insert_many(payload, ordered=False)
            elif kind == "spenders": db.top_spenders.replace_one({"_id": "current"}, payload, upsert=True)
        except Exception as e:
            print("Atlas write error:", str(e)[:120])
if ATLAS_URI:
    threading.Thread(target=atlas_writer, daemon=True).start()
else:
    print("MONGO_ATLAS_URI not set -> writing to local Mongo only.")

def to_atlas(kind, payload):
    if ATLAS_URI:
        try: atlas_q.put_nowait((kind, payload))
        except queue.Full: pass                      # never let the cloud slow the stream

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "zzz-mongo-sink", "enable.auto.commit": True,
              "auto.offset.reset": "earliest", "fetch.min.bytes": 65536})
c.subscribe([TOPIC])
spend, roll = Counter(), Counter()
seen = written = 0
t_roll = t_spend = t_print = time.time()
samples, sample_sec = [], int(time.time())
print("Sink running. Ctrl+C to stop.")
try:
    while True:
        msgs = c.consume(num_messages=5000, timeout=0.5)
        docs = []
        for m in msgs:
            if m.error(): continue
            e = json.loads(m.value()); e["ts_dt"] = datetime.fromtimestamp(e["ts"] / 1000, timezone.utc)
            docs.append(e); seen += 1
            roll["events"] += 1; roll["bytes"] += len(m.value())
            if e["event_type"] == "pull":
                roll["pulls"] += 1; roll[e["rank"] + "_rank"] += 1
            else:
                roll["topups"] += 1; roll["revenue_php"] += e["price_php"]; spend[e["username"]] += e["price_php"]
            if seen % SAMPLE_EVERY == 0:
                if sample_sec != int(time.time()): sample_sec, samples = int(time.time()), []
                if len(samples) < ATLAS_SAMPLE_CAP: samples.append(dict(e))
        if docs:
            try: local.insert_many(docs, ordered=False); written += len(docs)
            except BulkWriteError as ex: written += ex.details.get("nInserted", 0)
        t = time.time()
        if t - t_roll >= 1:
            if roll["events"]: to_atlas("rollup", {"ts_dt": now(), **roll})
            if samples: to_atlas("sample", samples); samples = []
            roll.clear(); t_roll = t
        if t - t_spend >= 10 and spend:
            to_atlas("spenders", {"_id": "current", "updated": now(), "top": spend.most_common(20)}); t_spend = t
        if t - t_print >= 5:
            print(f"consumed={seen:,} local_inserted={written:,} atlas_queue={atlas_q.qsize()}", flush=True); t_print = t
except KeyboardInterrupt:
    c.close()
