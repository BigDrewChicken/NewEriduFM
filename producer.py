#!/usr/bin/env python3
"""
ZZZ pull-stream producer.

Generates synthetic Zenless Zone Zero gacha events (pulls + top-ups) and sends
them as JSON to the Kafka topic `zzz.events` (6 partitions, created if missing).

Examples
--------
  python producer.py --dry-run --rate 20000          # generator speed test, no Kafka
  python producer.py --rate 1000 --duration 60       # gentle first test
  python producer.py --rate 10000 --start-rate 1000 --ramp 60 --duration 300

Needs:  pip install confluent-kafka python-dotenv
Prices, drop rates and item names are ILLUSTRATIVE, not official ZZZ numbers.
"""
import argparse
import json
import os
import random
import sys
import time
import uuid
from bisect import bisect
from itertools import accumulate, count

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "zzz.events")
PARTITIONS = 6
TICK = 0.05              # seconds per send loop
TOPUP_PROB = 0.07        # chance that a "user action" is a top-up instead of a pull
TEN_PULL_PROB = 0.40     # chance that a pull action is a 10-pull batch
POLYCHROME_PER_PULL = 160

# ----------------------------------------------------------------------------
# Static data (placeholder names; swap in your own items list if you have one)
# ----------------------------------------------------------------------------
BANNERS = [
    {"name": "Exclusive Channel: Ellen Joe", "kind": "Agent", "weight": 30, "s": "Ellen Joe",
     "a": ["Nicole Demara", "Ben Bigger", "Anton Ivanov"]},
    {"name": "Exclusive Channel: Zhu Yuan", "kind": "Agent", "weight": 25, "s": "Zhu Yuan",
     "a": ["Soukaku", "Corin Wickes", "Billy Kid"]},
    {"name": "W-Engine Channel: Deep Sea Visitor", "kind": "W-Engine", "weight": 25,
     "s": "Deep Sea Visitor", "a": ["The Brimstone", "Demara Battery Mark II", "Street Superstar"]},
    {"name": "W-Engine Channel: Cannon Rotor", "kind": "W-Engine", "weight": 10,
     "s": "Cannon Rotor", "a": ["The Restrained", "Electro-Lip Gloss", "Spring Embrace"]},
    {"name": "Stable Channel", "kind": "Agent", "weight": 10, "s": "Grace Howard",
     "a": ["Anby Demara", "Nekomata", "Lucy"]},
]
STD_S = {
    "Agent": ["Grace Howard", "Koleda Belobog", "Lycaon", "Soldier 11", "Rina"],
    "W-Engine": ["Steel Cushion", "Hellfire Gears", "Precious Fossilized Core", "Kaboom the Cannon"],
}
STD_A = {
    "Agent": ["Anby Demara", "Nekomata", "Lucy", "Piper Wheel", "Qingyi", "Seth Lowell"],
    "W-Engine": ["Bashful Demon", "Drill Rig - Red Axis", "Original Transmorpher", "Gilded Blossom"],
}
PACKS = [  # (name, polychrome, price_php)  illustrative
    ("Pack 60", 60, 59),
    ("Pack 330", 330, 299),
    ("Pack 1090", 1090, 929),
    ("Pack 2240", 2240, 1849),
    ("Pack 3880", 3880, 2999),
    ("Pack 8080", 8080, 5699),
]
PACK_W_NORMAL = [20, 30, 25, 15, 7, 3]
PACK_W_WHALE = [2, 8, 15, 25, 25, 25]
PAYMENTS = ["GCash", "Maya", "Card", "GrabPay"]
PAYMENT_W = [45, 20, 20, 15]

RUN = uuid.uuid4().hex[:6]   # keeps ids unique across runs

ITEM_IDS = {}
for _n in ([b["s"] for b in BANNERS] + [x for b in BANNERS for x in b["a"]]
           + [x for v in STD_S.values() for x in v] + [x for v in STD_A.values() for x in v]):
    ITEM_IDS.setdefault(_n, 1001 + len(ITEM_IDS))


# ----------------------------------------------------------------------------
# Generator
# ----------------------------------------------------------------------------
def build_users(n=1200, seed=7):
    rng = random.Random(seed)
    first = ["Neon", "Tape", "Reel", "Proxy", "Belle", "Wise", "Hollow", "Cunning", "Rusty", "Mixtape",
             "Eridu", "Fairy", "Bangboo", "Static", "Vinyl", "Arcade", "Retro", "Turbo", "Pixel", "Lucky"]
    second = ["Fox", "Cat", "Owl", "Rabbit", "Wolf", "Bear", "Crow", "Moth", "Shark", "Frog",
              "Hare", "Lynx", "Panda", "Toad", "Hawk", "Mole", "Seal", "Wasp", "Mouse", "Deer"]
    seen, users = set(), []
    while len(users) < n:
        name = f"{rng.choice(first)}{rng.choice(second)}{rng.randint(1, 999)}"
        if name not in seen:
            seen.add(name)
            users.append({"username": name, "id": 10_000_000 + len(users)})
    # heavy-tailed activity: a few whales, a long tail of casual players
    weights = [1.0 / ((i + 1) ** 0.9) for i in range(n)]
    return users, list(accumulate(weights))


def roll_one(rng, banner, st):
    """One pull. st = [pity_counter, guaranteed_featured]. Returns S/A only."""
    st[0] += 1
    pity = st[0]
    if pity >= 90:
        chance = 1.0
    elif pity >= 74:
        chance = 0.006 + (pity - 73) * 0.06
    else:
        chance = 0.006
    kind = banner["kind"]
    if rng.random() < chance:
        st[0] = 0
        if st[1] or rng.random() < 0.5:
            st[1] = False
            return "S", banner["s"], kind, True, pity
        st[1] = True
        return "S", rng.choice(STD_S[kind]), kind, False, pity
    if rng.random() < 0.5:
        return "A", rng.choice(banner["a"]), kind, True, pity
    return "A", rng.choice(STD_A[kind]), kind, False, pity


def event_stream(seed=None):
    """Infinite generator of event dicts. 'ts' is filled in by the send loop."""
    rng = random.Random(seed)
    users, cum = build_users()
    total_w = cum[-1]
    n_users = len(users)
    banner_cum = list(accumulate(b["weight"] for b in BANNERS))
    pity = {}
    pull_counts = [0] * n_users
    pull_ctr, batch_ctr = count(1), count(1)

    while True:
        u = min(bisect(cum, rng.random() * total_w), n_users - 1)
        user = users[u]

        if rng.random() < TOPUP_PROB:
            weights = PACK_W_WHALE if u < 100 else PACK_W_NORMAL
            pack, poly, price = rng.choices(PACKS, weights=weights)[0]
            yield {
                "event_type": "topup",
                "event_id": f"{RUN}-T{uuid.uuid4().hex[:12]}",
                "ts": None,
                "username": user["username"],
                "id": user["id"],
                "pack": pack,
                "polychrome": poly,
                "price_php": price,
                "payment": rng.choices(PAYMENTS, weights=PAYMENT_W)[0],
            }
            continue

        b_idx = bisect(banner_cum, rng.random() * banner_cum[-1])
        b_idx = min(b_idx, len(BANNERS) - 1)
        banner = BANNERS[b_idx]
        st = pity.setdefault((u, b_idx), [0, False])
        size = 10 if rng.random() < TEN_PULL_PROB else 1
        rolled = [roll_one(rng, banner, st) for _ in range(size)]
        batch_ranks = [r[0] for r in rolled]
        batch_id = f"{RUN}-B{next(batch_ctr):09d}"

        for rank, name, kind, featured, p in rolled:
            pull_counts[u] += 1
            yield {
                "event_type": "pull",
                "ts": None,
                "username": user["username"],
                "id": user["id"],
                "banner": banner["name"],
                "pull_number": pull_counts[u],
                "pull_id": f"{RUN}-P{next(pull_ctr):010d}",
                "batch_id": batch_id,
                "batch_size": size,
                "item_id": ITEM_IDS[name],
                "item_name": name,
                "item_type": kind,
                "rank": rank,
                "pity_count": p,
                "is_featured": featured,
                "polychrome_spent": POLYCHROME_PER_PULL,
                "batch_ranks": batch_ranks,
            }


# ----------------------------------------------------------------------------
# Kafka helpers
# ----------------------------------------------------------------------------
def connect_kafka():
    try:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError:
        sys.exit("Missing package. Run: pip install confluent-kafka python-dotenv")

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        md = admin.list_topics(timeout=8)
    except Exception:
        sys.exit(f"Cannot reach Kafka at {BOOTSTRAP}. Is Docker Desktop running? "
                 f"Try: docker compose ps  /  docker logs zzz-kafka")

    if TOPIC not in md.topics:
        fs = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])
        for _, f in fs.items():
            try:
                f.result()
                print(f"Created topic {TOPIC} ({PARTITIONS} partitions)")
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    raise

    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 262144,
        "compression.type": "none",
        "acks": 1,
    })


delivery_errors = 0


def on_delivery(err, _msg):
    global delivery_errors
    if err is not None:
        delivery_errors += 1


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------
def target_rate(elapsed, a):
    if a.ramp > 0 and elapsed < a.ramp:
        return a.start_rate + (a.rate - a.start_rate) * elapsed / a.ramp
    return a.rate


def main():
    ap = argparse.ArgumentParser(description="ZZZ pull-stream Kafka producer")
    ap.add_argument("--rate", type=float, default=1000, help="target events/sec (default 1000)")
    ap.add_argument("--start-rate", type=float, default=None, help="events/sec at start of ramp")
    ap.add_argument("--ramp", type=float, default=0, help="seconds to ramp from start-rate to rate")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl+C)")
    ap.add_argument("--dry-run", action="store_true", help="generate and serialize only, no Kafka")
    ap.add_argument("--sample", type=int, default=3, help="print N sample events at start (default 3)")
    ap.add_argument("--seed", type=int, default=None, help="random seed for repeatable data")
    a = ap.parse_args()
    if a.start_rate is None:
        a.start_rate = a.rate

    gen = event_stream(a.seed)

    # show a few events so you can see what the data looks like
    if a.sample > 0:
        print(f"--- {a.sample} sample events ---")
        sample_gen = event_stream(a.seed)
        for _ in range(a.sample):
            ev = next(sample_gen)
            ev["ts"] = int(time.time() * 1000)
            print(json.dumps(ev))
        print("-------------------------")

    producer = None if a.dry_run else connect_kafka()
    mode = "DRY RUN (no Kafka)" if a.dry_run else f"-> {BOOTSTRAP} topic={TOPIC}"
    print(f"Producer started {mode}. Ctrl+C to stop.")

    t0 = time.perf_counter()
    next_tick = t0
    next_report = t0 + 1
    carry = 0.0
    total = total_bytes = sec_events = sec_bytes = 0

    try:
        while True:
            now = time.perf_counter()
            elapsed = now - t0
            if a.duration and elapsed >= a.duration:
                break

            carry += target_rate(elapsed, a) * TICK
            n = int(carry)
            carry -= n
            ts = int(time.time() * 1000)   # epoch milliseconds (sink.py expects a number)

            for _ in range(n):
                ev = next(gen)
                ev["ts"] = ts
                payload = json.dumps(ev, separators=(",", ":")).encode("utf-8")
                sec_events += 1
                sec_bytes += len(payload)
                if producer is not None:
                    while True:
                        try:
                            producer.produce(TOPIC, payload, key=ev["username"].encode("utf-8"),
                                             on_delivery=on_delivery)
                            break
                        except BufferError:
                            producer.poll(0.05)
            if producer is not None:
                producer.poll(0)

            now = time.perf_counter()
            if now >= next_report:
                span = now - (next_report - 1)
                total += sec_events
                total_bytes += sec_bytes
                print(f"t={now - t0:5.0f}s  target={target_rate(now - t0, a):8,.0f}  "
                      f"actual={sec_events / span:8,.0f} events/s  "
                      f"{sec_bytes / span / 1e6:5.2f} MB/s  total={total:,}"
                      + (f"  delivery_errors={delivery_errors}" if delivery_errors else ""))
                sec_events = sec_bytes = 0
                next_report += 1

            next_tick += TICK
            delay = next_tick - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            else:
                next_tick = time.perf_counter()   # behind schedule (CPU-bound): don't pile up
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        total += sec_events
        total_bytes += sec_bytes
        if producer is not None:
            remaining = producer.flush(30)
            if remaining:
                print(f"Warning: {remaining} messages were not delivered")
        secs = max(time.perf_counter() - t0, 1e-9)
        print(f"Done. {total:,} events, {total_bytes / 1e6:,.1f} MB in {secs:,.0f}s "
              f"(avg {total / secs:,.0f} events/s)")


if __name__ == "__main__":
    main()