"""
Real-Time Healthcare Data Processing - Base Project
====================================================
Pipeline:  Simulated patient monitors  ->  Queue (stream)  ->  Processor
                                                               |- validation
                                                               |- rolling averages
                                                               |- rule-based alerts
                                                               |- statistical anomaly detection (z-score)
                                                               |- SQLite storage

Only the Python standard library is used (Python 3.8+).

Run:
    python healthcare_stream.py                  # 20 seconds, 5 patients
    python healthcare_stream.py -d 60 -p 10      # 60 seconds, 10 patients
"""

import argparse
import queue
import random
import sqlite3
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime

# --------------------------------------------------------------------------
# 1. Data model
# --------------------------------------------------------------------------

@dataclass
class VitalsReading:
    patient_id: str
    timestamp: str
    heart_rate: float      # beats per minute
    spo2: float            # oxygen saturation, %
    temperature: float     # degrees Celsius
    systolic_bp: float     # mmHg
    diastolic_bp: float    # mmHg


# Clinical thresholds (simplified, for demonstration only - NOT medical advice)
THRESHOLDS = {
    "heart_rate":   (50, 120),
    "spo2":         (92, 100),
    "temperature":  (35.5, 38.0),
    "systolic_bp":  (90, 140),
    "diastolic_bp": (60, 90),
}

# Physically plausible ranges used to reject sensor errors
VALID_RANGE = {
    "heart_rate":   (20, 250),
    "spo2":         (50, 100),
    "temperature":  (30, 43),
    "systolic_bp":  (50, 260),
    "diastolic_bp": (30, 160),
}

# --------------------------------------------------------------------------
# 2. Producer: simulated bedside monitors
# --------------------------------------------------------------------------

class PatientSimulator(threading.Thread):
    """Generates a stream of vitals for several patients."""

    def __init__(self, out_queue, patient_ids, interval, stop_event):
        super().__init__(daemon=True)
        self.q = out_queue
        self.patients = patient_ids
        self.interval = interval
        self.stop_event = stop_event
        # Each patient has their own "baseline"
        self.baseline = {
            pid: {
                "heart_rate": random.uniform(65, 85),
                "spo2": random.uniform(96, 99),
                "temperature": random.uniform(36.4, 37.1),
                "systolic_bp": random.uniform(105, 125),
                "diastolic_bp": random.uniform(68, 80),
            }
            for pid in patient_ids
        }

    def _generate(self, pid):
        b = self.baseline[pid]
        r = VitalsReading(
            patient_id=pid,
            timestamp=datetime.now().isoformat(timespec="milliseconds"),
            heart_rate=random.gauss(b["heart_rate"], 3),
            spo2=min(100, random.gauss(b["spo2"], 0.7)),
            temperature=random.gauss(b["temperature"], 0.1),
            systolic_bp=random.gauss(b["systolic_bp"], 4),
            diastolic_bp=random.gauss(b["diastolic_bp"], 3),
        )
        # Occasionally inject a medical event or a faulty sensor value
        roll = random.random()
        if roll < 0.03:      # tachycardia + low oxygen
            r.heart_rate += random.uniform(45, 70)
            r.spo2 -= random.uniform(5, 10)
        elif roll < 0.05:    # fever
            r.temperature += random.uniform(1.5, 2.5)
        elif roll < 0.07:    # hypertensive spike
            r.systolic_bp += random.uniform(40, 60)
            r.diastolic_bp += random.uniform(20, 30)
        elif roll < 0.08:    # sensor glitch (invalid value)
            r.heart_rate = random.choice([0, 400])
        return r

    def run(self):
        while not self.stop_event.is_set():
            for pid in self.patients:
                self.q.put(self._generate(pid))
            time.sleep(self.interval)

# --------------------------------------------------------------------------
# 3. Storage
# --------------------------------------------------------------------------

class Storage:
    """SQLite persistence for readings and alerts."""

    def __init__(self, path="healthcare.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS vitals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT, timestamp TEXT,
                    heart_rate REAL, spo2 REAL, temperature REAL,
                    systolic_bp REAL, diastolic_bp REAL
                );
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT, timestamp TEXT,
                    severity TEXT, metric TEXT, message TEXT
                );
                """
            )

    def save_vitals(self, r):
        with self.lock:
            self.conn.execute(
                "INSERT INTO vitals (patient_id, timestamp, heart_rate, spo2, "
                "temperature, systolic_bp, diastolic_bp) VALUES (?,?,?,?,?,?,?)",
                (r.patient_id, r.timestamp, r.heart_rate, r.spo2,
                 r.temperature, r.systolic_bp, r.diastolic_bp),
            )
            self.conn.commit()

    def save_alert(self, pid, ts, severity, metric, message):
        with self.lock:
            self.conn.execute(
                "INSERT INTO alerts (patient_id, timestamp, severity, metric, message) "
                "VALUES (?,?,?,?,?)",
                (pid, ts, severity, metric, message),
            )
            self.conn.commit()

    def count(self, table):
        with self.lock:
            return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def alerts_by_patient(self):
        with self.lock:
            return self.conn.execute(
                "SELECT patient_id, COUNT(*) FROM alerts GROUP BY patient_id ORDER BY 2 DESC"
            ).fetchall()

    def close(self):
        with self.lock:
            self.conn.close()

# --------------------------------------------------------------------------
# 4. Consumer: stream processor
# --------------------------------------------------------------------------

class StreamProcessor(threading.Thread):
    WINDOW = 20          # readings kept per patient/metric for rolling stats
    Z_LIMIT = 3.0        # z-score above which a value is "statistically unusual"

    def __init__(self, in_queue, storage, stop_event):
        super().__init__(daemon=True)
        self.q = in_queue
        self.db = storage
        self.stop_event = stop_event
        self.windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=self.WINDOW)))
        self.stats = {"processed": 0, "rejected": 0, "alerts": 0}

    # ---- steps ----------------------------------------------------------
    @staticmethod
    def is_valid(r):
        for metric, (lo, hi) in VALID_RANGE.items():
            if not lo <= getattr(r, metric) <= hi:
                return False, metric
        if r.systolic_bp <= r.diastolic_bp:
            return False, "blood_pressure"
        return True, None

    def raise_alert(self, r, severity, metric, message):
        self.stats["alerts"] += 1
        self.db.save_alert(r.patient_id, r.timestamp, severity, metric, message)
        print(f"  [{severity:<8}] {r.timestamp[11:23]}  {r.patient_id}  {message}")

    def check_thresholds(self, r):
        for metric, (lo, hi) in THRESHOLDS.items():
            value = getattr(r, metric)
            if value < lo or value > hi:
                # Critical when far outside the safe band
                span = hi - lo
                far = value < lo - 0.25 * span or value > hi + 0.25 * span
                severity = "CRITICAL" if far else "WARNING"
                self.raise_alert(
                    r, severity, metric,
                    f"{metric}={value:.1f} outside safe range ({lo}-{hi})",
                )

    def check_anomaly(self, r):
        """Flag values that deviate sharply from the patient's own recent history."""
        for metric in THRESHOLDS:
            value = getattr(r, metric)
            window = self.windows[r.patient_id][metric]
            if len(window) >= 10:
                mean = statistics.mean(window)
                sd = statistics.pstdev(window) or 1e-6
                z = (value - mean) / sd
                if abs(z) > self.Z_LIMIT:
                    # Only report anomalies the threshold rule did not already catch
                    lo, hi = THRESHOLDS[metric]
                    if lo <= value <= hi:
                        self.raise_alert(
                            r, "NOTICE", metric,
                            f"{metric}={value:.1f} unusual for patient (z={z:+.1f}, avg={mean:.1f})",
                        )
            window.append(value)

    def process(self, r):
        ok, bad_field = self.is_valid(r)
        if not ok:
            self.stats["rejected"] += 1
            print(f"  [REJECTED ] {r.patient_id}  invalid sensor value for {bad_field}")
            return
        self.check_thresholds(r)
        self.check_anomaly(r)
        self.db.save_vitals(r)
        self.stats["processed"] += 1

    # ---- main loop ------------------------------------------------------
    def run(self):
        while not self.stop_event.is_set() or not self.q.empty():
            try:
                reading = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            self.process(reading)
            self.q.task_done()

# --------------------------------------------------------------------------
# 5. Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Real-time healthcare data processing demo")
    ap.add_argument("-d", "--duration", type=int, default=20, help="seconds to run")
    ap.add_argument("-p", "--patients", type=int, default=5, help="number of patients")
    ap.add_argument("-i", "--interval", type=float, default=0.5, help="seconds between readings")
    ap.add_argument("--db", default="healthcare.db", help="SQLite file path")
    args = ap.parse_args()

    patient_ids = [f"P{str(i).zfill(3)}" for i in range(1, args.patients + 1)]
    stream = queue.Queue()
    stop = threading.Event()
    db = Storage(args.db)

    producer = PatientSimulator(stream, patient_ids, args.interval, stop)
    processor = StreamProcessor(stream, db, stop)

    print(f"Monitoring {len(patient_ids)} patients for {args.duration}s (Ctrl+C to stop)\n")
    producer.start()
    processor.start()

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        stop.set()
        producer.join(timeout=2)
        processor.join(timeout=5)

    print("\n=========== SUMMARY ===========")
    print(f"Readings processed : {processor.stats['processed']}")
    print(f"Readings rejected  : {processor.stats['rejected']}")
    print(f"Alerts raised      : {processor.stats['alerts']}")
    print(f"Rows in DB         : vitals={db.count('vitals')}, alerts={db.count('alerts')}")
    print("Alerts per patient :", dict(db.alerts_by_patient()))
    db.close()


if __name__ == "__main__":
    main()
