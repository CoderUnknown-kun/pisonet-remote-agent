import threading
import time
import os
import sys
import socket
import psutil
import ctypes
import io
import uuid
import json
import urllib.request
from datetime import datetime, timezone
import flask

from flask import Flask, Response
import mss
from PIL import Image

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import db as rtdb

import asyncio
from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
from aiortc.sdp import candidate_from_sdp
import av

import hashlib

import requests
import subprocess

AGENT_VERSION = "1.0.6"

CURRENT_VERSION = AGENT_VERSION
VERSION_URL = "https://mlsn-industries.web.app/version.json"
def verify_self_integrity():
    try:
        exe_path = sys.executable if getattr(sys, "frozen", False) else __file__
        actual = compute_sha256(exe_path)

        response = requests.get(VERSION_URL, timeout=5)
        expected = response.json()["sha256"]

        if actual.lower() != expected.lower():
            log("WARNING: running binary hash mismatch")
        else:
            log("Binary integrity verified")

    except Exception as e:
        log(f"Integrity check failed: {e}")

def check_for_update():
    try:
        response = requests.get(VERSION_URL, timeout=5)
        data = response.json()

        latest_version = data["version"]
        expected_hash = data["sha256"]
        download_url = data["url"]

        from packaging import version
        if version.parse(latest_version) > version.parse(CURRENT_VERSION):
            log(f"New version found: {latest_version}")
            download_update(download_url, expected_hash)

    except Exception as e:
        log(f"Update check failed: {e}")

def download_update(download_url, expected_hash):
    try:
        new_exe_path = os.path.join(app_dir(), "pisonet_agent_new.exe")

        log("Downloading update...")

        r = requests.get(download_url, stream=True, timeout=60)

        if r.status_code != 200:
            log(f"Download failed: status {r.status_code}")
            return

        with open(new_exe_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)

        log("Download complete")

        # VERIFY HASH
        actual_hash = compute_sha256(new_exe_path)

        if actual_hash.lower() != expected_hash.lower():
            log("SHA256 mismatch! Update aborted.")
            os.remove(new_exe_path)
            return

        log("Update verified")

        updater = os.path.join(app_dir(), "updater.exe")

        if not os.path.exists(updater):
            log("Updater.exe missing")
            return

        log("Launching updater and exiting agent")

        subprocess.Popen(
            [updater],
            cwd=app_dir(),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )

        time.sleep(1)
        os._exit(0)

    except Exception as e:
        log(f"Download/update failed: {e}")

def compute_sha256(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
# =====================================================
# MINIMAL AGENT LOGGER
# =====================================================
LOG_FILE = None
LOG_LOCK = threading.Lock()

def log(msg):
    global LOG_FILE
    try:
        if LOG_FILE is None:
            LOG_FILE = os.path.join(os.environ["ProgramData"], "PisonetAgent", "agent.log")
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        with LOG_LOCK:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
                )
    except:
        pass

# =====================================================
# safe base-path resolver
# =====================================================
def app_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

# =====================================================
# STABLE PC ID (PERSISTENT, BOOT-SAFE)
# =====================================================
def get_or_create_pc_id():
    try:
        id_path = os.path.join(app_dir(), "pc_id.txt")

        if os.path.exists(id_path):
            with open(id_path, "r", encoding="utf-8") as f:
                return f.read().strip()

        new_id = str(uuid.uuid4())
        with open(id_path, "w", encoding="utf-8") as f:
            f.write(new_id)

        return new_id
    except:
        return os.environ.get("COMPUTERNAME", "UNKNOWN_PC")
# =====================================================
# MJPEG QUALITY PROFILES (LOW BANDWIDTH SAFE)
# =====================================================
MJPEG_PROFILES = {
    "normal": {
        "fps": 6,
        "quality": 45,
        "scale": 1.0,
    },
    "low": {
        "fps": 2,
        "quality": 30,
        "scale": 0.6,
    }
}

DEFAULT_MJPEG_PROFILE = "normal"

IDLE_FINALIZE_SECONDS = 0  # 5 minutes idle = finalize
idle_start_ts = None

# =====================================================
# CONFIG
# =====================================================
PC_ID = get_or_create_pc_id()
FLASK_PORT = 5800

HEARTBEAT_INTERVAL = 30
HEARTBEAT_FAIL_TIMEOUT = 90

PESO_SECONDS = 300  # ₱1 = 300 seconds

APP_DIR = app_dir()

FIREBASE_KEY = os.path.join(APP_DIR, "serviceAccountKey.json")
STATE_FILE = os.path.join(APP_DIR, "session_state.json")
AUDIT_FILE = os.path.join(APP_DIR, "audit.log")

LHM_URL = "http://127.0.0.1:8085/data.json"

# =====================================================
# ANTI-DUPLICATE (Windows Mutex) ✅ PRODUCTION SAFE
# =====================================================
if sys.platform == "win32":
    mutex = ctypes.windll.kernel32.CreateMutexW(
        None,
        False,
        f"PISONET_AGENT_MUTEX_{PC_ID}"
    )

    if ctypes.windll.kernel32.GetLastError() == 183:
        # another instance already running
        sys.exit(0)
        
# =====================================================
# HIDE CONSOLE (Windows)
# =====================================================
if sys.platform == "win32":
    try:
        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)
    except:
        pass
# =====================================================
# COMPANY CONFIG (MULTI-TENANT SUPPORT)
# =====================================================
COMPANY_ID = "mlsn_internal"
# =====================================================
# FIREBASE INIT
# =====================================================
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(
        cred,
        {
            "databaseURL":
            "https://mlsn-industries-default-rtdb.asia-southeast1.firebasedatabase.app"
        }
    )

db = firestore.client()
pc_ref = (
    db.collection("companies")
      .document(COMPANY_ID)
      .collection("pcs")
      .document(PC_ID)
)
# TEMPORARY fallback mirror (old structure)
old_pc_ref = db.collection("pcs").document(PC_ID)

rtdb_ref = rtdb.reference("telemetry").child(PC_ID)
import atexit

def cleanup_rtdb():
    try:
        if 'rtdb_ref' in globals():
            rtdb_ref.child("isOnline").set(False)
        if 'pc_ref' in globals():
            pc_ref.set({"isOnline": False}, merge=True)
            old_pc_ref.set({"isOnline": False}, merge=True)
    except:
        pass


atexit.register(cleanup_rtdb)
boot_block_session_start = True

pcs = set()
webrtc_loop = None
# =====================================================
# GLOBALS
# =====================================================
stop_event = threading.Event()
app = Flask(__name__)
last_temp = None

session_seconds = 0
session_active = False
session_locked = False
last_tick = time.time()
last_rollover_date = None

agent_boot_id = str(uuid.uuid4())
session_seq = 0
last_heartbeat_ok = time.time()

current_session = {
    "id": None,
    "started_at": None,
    "started_at_local": None,
    "start_seconds": 0,
    "audit": {}
}

last_end_reason = None
audit_last_hash = "GENESIS"
audit_counter = 0
# =====================================================
# MESSAGE CONTROL (NON-BLOCKING)
# =====================================================
message_active = False
message_lock = threading.Lock()
heartbeat_thread = None
mjpeg_thread = None

# =====================================================
# STATE PERSISTENCE
# =====================================================
def load_state():
    global session_seconds, last_tick, current_session, last_rollover_date
    global session_seq, last_end_reason

    try:
        if not os.path.exists(STATE_FILE):
            return

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        session_seconds = int(data.get("session_seconds", 0))
        last_tick = float(data.get("last_tick", time.time()))
        session_seq = int(data.get("session_seq", 0))
        current_session.update(data.get("current_session", {}))
        last_end_reason = data.get("last_end_reason")

        # 🔐 SESSION RECOVERY MARK
        if current_session.get("id"):
            current_session.setdefault("audit", {})
            current_session["audit"]["recovered"] = True

        last_rollover_date = (
            datetime.fromisoformat(data["last_rollover_date"]).date()
            if data.get("last_rollover_date")
            else None
        )

        # =====================================================
        # 🔥 HARD REBOOT / POWER-LOSS GUARD (FINAL FIX)
        # =====================================================
        if current_session.get("id"):
            saved_boot = current_session.get("audit", {}).get("agentBootId")
            if saved_boot and saved_boot != agent_boot_id:
                try:
                    started = current_session.get("started_at_local")
                    if started:
                        used_seconds = int((now_utc_safe() - started).total_seconds())
                    else:
                        used_seconds = 0

                    if used_seconds > 0:
                        log(f"Recovered session after crash: {used_seconds}s → ₱{used_seconds // PESO_SECONDS}")

                        pc_ref.collection("sessions").document(current_session["id"]).set({
                            "pcId": PC_ID,
                            "startedAt": current_session.get("started_at"),
                            "startedAtLocal": current_session.get("started_at_local"),
                            "endedAt": firestore.SERVER_TIMESTAMP,
                            "finalizedAtLocal": datetime.now().astimezone(timezone.utc).replace(tzinfo=None),
                            "durationSeconds": used_seconds,
                            "derivedPeso": used_seconds // PESO_SECONDS,
                            "endReason": "reboot_or_power_loss",
                            "createdBy": "agent",
                            "createdAt": firestore.SERVER_TIMESTAMP,
                            "agentVersion": AGENT_VERSION,
                            "audit": {
                                **current_session.get("audit", {}),
                                "endedBy": "system",
                                "endReason": "reboot_or_power_loss"
                            }
                        })
                except:
                    pass

                current_session.update({
                    "id": None,
                    "started_at": None,
                    "started_at_local": None,
                    "start_seconds": 0,
                    "audit": {}
                })
                session_seconds = 0
                last_tick = time.time()
                save_state()

    except:
        pass

def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "session_seconds": session_seconds,
                "last_tick": last_tick,
                "session_seq": session_seq,
                "current_session": current_session,
                "last_rollover_date":
                    last_rollover_date.isoformat() if last_rollover_date else None,
                "last_end_reason": last_end_reason
            }, f)
    except:
        pass

# =====================================================
# AUDIT LEDGER (FULL MERGE TRUST LAYER)
# =====================================================
def restore_audit_chain():
    global audit_last_hash, audit_counter

    try:
        if not os.path.exists(AUDIT_FILE):
            return

        with open(AUDIT_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()

            if size == 0:
                return

            # Read last ~4KB max
            f.seek(max(size - 4096, 0))
            lines = f.read().decode("utf-8", errors="ignore").splitlines()
            lines = [l for l in lines if l.strip()]

            if not lines:
                return

            last_line = lines[-1]
            try:
                last_entry = json.loads(last_line)
            except:
                return

            audit_last_hash = last_entry.get("hash", "GENESIS")
            audit_counter = last_entry.get("seq", 0)

    except Exception as e:
        log(f"Audit restore failed: {e}")
        audit_last_hash = "GENESIS"
        audit_counter = 0

def sync_integrity_now():
    try:
        payload = {
            "integrity": {
                "auditCounter": audit_counter,
                "lastAuditHash": audit_last_hash,
                "bootId": agent_boot_id
            }
        }

        pc_ref.set(payload, merge=True)
        old_pc_ref.set(payload, merge=True)   # ← add this

    except Exception as e:
        log(f"Integrity sync failed: {e}")

def append_audit(event, details=None):
    global audit_last_hash, audit_counter

    try:
        audit_counter += 1

        payload = {
            "ts": int(time.time()),
            "seq": audit_counter,
            "event": event,
            "details": details or {},
            "prevHash": audit_last_hash
        }

        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        new_hash = hashlib.sha256(raw.encode()).hexdigest()

        payload["hash"] = new_hash
        audit_last_hash = new_hash

        with open(AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except:
                pass

    except Exception as e:
        log(f"Audit write failed: {e}")

def summarize_audit(limit=50):
    try:
        if not os.path.exists(AUDIT_FILE):
            return {
                "events": 0,
                "lastEventAt": int(time.time()),
                "lines": []
            }

        with open(AUDIT_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-limit:]

        last_entry = json.loads(lines[-1])

        return {
            "events": len(lines),
            "lastEventAt": last_entry.get("ts", int(time.time())),
            "lines": [l.strip() for l in lines]
        }
    except Exception as e:
        log(f"Audit summarize failed: {e}")
        return None
# =====================================================
# CPU TEMPERATURE (AMD + INTEL SAFE)
# =====================================================
def get_cpu_temperature():
    try:
        with urllib.request.urlopen(LHM_URL, timeout=2) as r:
            data = json.loads(r.read().decode("utf-8"))

        candidates = []

        def walk(node):
            if isinstance(node, dict):
                text = str(node.get("Text", "")).lower()
                value = node.get("Value")

                if isinstance(value, str) and "°c" in value.lower():
                    try:
                        temp = float(
                            value.replace("°C", "")
                                 .replace("°c", "")
                                 .strip()
                        )

                        if "tctl" in text or "tdie" in text:
                            candidates.append((0, temp))   # AMD
                        elif "package" in text:
                            candidates.append((1, temp))   # Intel
                        elif "core max" in text:
                            candidates.append((2, temp))
                        elif "core average" in text:
                            candidates.append((3, temp))
                        else:
                            candidates.append((4, temp))   # fallback
                    except:
                        pass

                for c in node.get("Children", []):
                    walk(c)

            elif isinstance(node, list):
                for i in node:
                    walk(i)

        walk(data)

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[0])
        return round(candidates[0][1], 1)

    except:
        return None

# =====================================================
# TELEMETRY LOOP
# =====================================================
def telemetry_loop():
    global last_temp

    # Prime psutil (prevents first-read zero bug)
    psutil.cpu_percent(interval=None)

    while not stop_event.is_set():
        try:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent

            temp = get_cpu_temperature()

            if temp is not None:
                last_temp = temp
                temp_status = "ok"
            else:
                temp_status = "unavailable"

            # 🔥 ALWAYS WRITE RTDB (even if temp missing)
            now_ts = int(time.time())

            rtdb_ref.set({
                "pcId": PC_ID,
                "displayName": os.environ.get("COMPUTERNAME", PC_ID),
                "cpu": cpu,
                "ram": ram,
                "temp": last_temp,
                "tempStatus": temp_status,
                "updatedAt": now_ts,
                "ttl": now_ts + 300
            })


        except Exception as e:
            log(f"Telemetry error: {e}")


        time.sleep(5)

# =====================================================
# HELPERS
# =====================================================
class LASTINPUTINFO(ctypes.Structure):
    _fields_ = [("cbSize", ctypes.c_uint), ("dwTime", ctypes.c_uint)]

def is_monitor_on():
    try:
        lii = LASTINPUTINFO()
        lii.cbSize = ctypes.sizeof(lii)
        ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii))
        idle_ms = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
        return idle_ms < 5 * 60 * 1000
    except:
        return True
def has_user_activity():
    try:
        lii = LASTINPUTINFO()
        lii.cbSize = ctypes.sizeof(lii)
        ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii))
        idle_ms = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
        return idle_ms < 5 * 60 * 1000  # ✅ RETURN
    except:
        return True


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return ""

def now_utc_safe():
    return datetime.now().astimezone(timezone.utc).replace(tzinfo=None)

def is_new_day():
    global last_rollover_date
    if current_session["started_at_local"] is None:
        return False
    today = datetime.now().date()
    started_day = current_session["started_at_local"].date()
    if started_day != today and last_rollover_date != today:
        last_rollover_date = today
        return True
    return False

def handle_idle_transition():
    global session_active, session_seconds, last_tick, idle_start_ts

    if not current_session["id"]:
        idle_start_ts = None
        return

    if not is_monitor_on():
        # 🔥 IMMEDIATE FINALIZE MODE
        if IDLE_FINALIZE_SECONDS == 0:
            log("Idle detected → immediate finalize")
            end_session_if_active("idle_timeout")
            session_seconds = 0
            session_active = False
            last_tick = time.time()
            idle_start_ts = None
            save_state()
            return

        # ⏱️ DELAYED FINALIZE MODE
        if idle_start_ts is None:
            idle_start_ts = time.time()
            log("Idle started")
            return

        if time.time() - idle_start_ts >= IDLE_FINALIZE_SECONDS:
            log("Idle timeout reached → finalizing session")
            end_session_if_active("idle_timeout")
            session_seconds = 0
            session_active = False
            last_tick = time.time()
            idle_start_ts = None
            save_state()
    else:
        idle_start_ts = None

# =====================================================
# SESSION CONTROL
# =====================================================
def start_session_if_needed():
    global session_seq, last_end_reason

    if session_locked or current_session["id"]:
        return

    last_end_reason = None  # 🔑 CLEAR ONLY ON REAL START

    session_seq += 1
    current_session.update({
        "id": str(uuid.uuid4()),
        "started_at": firestore.SERVER_TIMESTAMP,
        "started_at_local": now_utc_safe(),
        "start_seconds": session_seconds,
        "audit": {
            "sessionSeq": session_seq,
            "agentBootId": agent_boot_id,
            "startedBy": "agent",
            "recovered": False
        }
    })
    save_state()

def end_session_if_active(reason):
    global last_end_reason

    if current_session["id"] is None:
        return

    used_seconds = session_seconds - current_session["start_seconds"]
    if used_seconds <= 0:
        reset_session()
        return

    pc_ref.collection("sessions").document(current_session["id"]).set({
        "pcId": PC_ID,
        "startedAt": current_session["started_at"],
        "startedAtLocal": current_session["started_at_local"],
        "endedAt": firestore.SERVER_TIMESTAMP,
        "finalizedAtLocal": now_utc_safe(),
        "durationSeconds": used_seconds,
        "derivedPeso": used_seconds // PESO_SECONDS,
        "endReason": reason,
        "createdBy": "agent",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "agentVersion": AGENT_VERSION,
        "audit": {
            **current_session.get("audit", {}),
            "endedBy": "agent",
            "endReason": reason
        }
    })

    last_end_reason = reason
    reset_session()

def reset_session():
    current_session.update({
        "id": None,
        "started_at": None,
        "started_at_local": None,
        "start_seconds": 0,
        "audit": {}
    })
    save_state()

def write_online_heartbeat():
    if audit_counter == 0:
        return
    try:
        audit_summary = summarize_audit()

        payload = {
            "displayName": os.environ.get("COMPUTERNAME", "PC"),
            "ip": get_local_ip(),
            "isOnline": True,
            "lastSeen": firestore.SERVER_TIMESTAMP,
            "agentAliveAt": firestore.SERVER_TIMESTAMP,
            "agentVersion": AGENT_VERSION,
            "session": {
                "seconds": session_seconds,
                "active": session_active,
                "locked": session_locked,
                "updatedAt": firestore.SERVER_TIMESTAMP
            },
            "integrity": {
                "auditCounter": audit_counter,
                "lastAuditHash": audit_last_hash,
                "bootId": agent_boot_id
            }
        }

        # 🔐 Only write audit if it exists
        if audit_summary is not None:
            payload["audit"] = audit_summary

        pc_ref.set(payload, merge=True)
        old_pc_ref.set(payload, merge=True)   # temporary mirror

        # 🔥 RTDB presence mirror
        rtdb_ref.child("isOnline").set(True)
        rtdb_ref.child("agentAliveAt").set(int(time.time()))

    except Exception as e:
        log(f"Heartbeat write failed: {e}")
# =====================================================
# HEARTBEAT
# =====================================================
def firebase_heartbeat():
    
    global session_seconds, session_active, last_tick, last_heartbeat_ok
    global boot_block_session_start

    while not stop_event.is_set():
        now = time.time()
        elapsed = max(0, int(now - last_tick))
        last_tick = now

        try:
            # 🔥 ALWAYS announce presence first (unconditional)
            write_online_heartbeat()

            # ─────────────────────────────────────
            # BOOT BARRIER (SESSION ONLY)
            # ─────────────────────────────────────
            if boot_block_session_start:
                if has_user_activity():
                    boot_block_session_start = False
                    last_tick = time.time()
                else:
                    session_active = False
                    save_state()
                    last_heartbeat_ok = time.time()
                    time.sleep(HEARTBEAT_INTERVAL)
                    continue

            # ─────────────────────────────────────
            # IDLE HANDLING (SESSION ONLY)
            # ─────────────────────────────────────
            if not is_monitor_on():
                handle_idle_transition()
                save_state()
                last_heartbeat_ok = time.time()
                time.sleep(HEARTBEAT_INTERVAL)
                continue

            # ─────────────────────────────────────
            # ACTIVE SESSION LOGIC
            # ─────────────────────────────────────
            if current_session["id"] and is_new_day():
                end_session_if_active("day_rollover")
                session_seconds = 0
                last_tick = time.time()

            if last_end_reason not in ("shutdown", "restart"):
                start_session_if_needed()

            session_active = True
            session_seconds += elapsed
            save_state()

            last_heartbeat_ok = time.time()

        except Exception as e:
            log(f"Heartbeat loop error (non-fatal): {e}")

            # 🔁 Fallback heartbeat attempt
            try:
                write_online_heartbeat()
            except:
                pass

            if time.time() - last_heartbeat_ok > (HEARTBEAT_FAIL_TIMEOUT * 10):
                log("Persistent heartbeat failure, exiting agent")
                os._exit(2)

        time.sleep(HEARTBEAT_INTERVAL)

class ScreenTrack(VideoStreamTrack):
    def __init__(self):
        super().__init__()
        self.sct = mss.mss()
        self.monitor = self.sct.monitors[1] if len(self.sct.monitors) > 1 else self.sct.monitors[0]
        self.last = time.time()

    async def recv(self):
        now = time.time()
        await asyncio.sleep(max(0, (1/8) - (now - self.last)))  # ~8 FPS
        self.last = time.time()

        img = self.sct.grab(self.monitor)
        frame = av.VideoFrame.from_ndarray(
            Image.frombytes("RGB", img.size, img.rgb).convert("RGB"),
            format="rgb24"
        )
        frame.pts, frame.time_base = None, None
        return frame

async def handle_webrtc_offer(offer_sdp):
    # 🔥 CLOSE OLD PEER CONNECTIONS FIRST
    for old in list(pcs):
        try:
            await old.close()
        except:
            pass
        pcs.discard(old)

    pc = RTCPeerConnection({
    "iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]
})
    pcs.add(pc)

    @pc.on("connectionstatechange")
    async def on_state_change():
        log(f"WebRTC state: {pc.connectionState}")
        if pc.connectionState in ("failed", "closed", "disconnected"):
            try:
                await pc.close()
            except:
                pass
            pcs.discard(pc)

    pc.addTransceiver("video", direction="sendonly")
    pc.addTrack(ScreenTrack())

    webrtc_inbound_ice_listener(pc)

    await pc.setRemoteDescription(
        RTCSessionDescription(sdp=offer_sdp, type="offer")
    )

    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)

    pc_ref.collection("webrtc").document("answer").set({
        "sdp": pc.localDescription.sdp,
        "type": pc.localDescription.type,
        "ts": firestore.SERVER_TIMESTAMP
    })
    # 🔥 CONSUME OFFER
    pc_ref.collection("webrtc").document("offer").delete()

def handle_offer_doc(doc_snapshot):
    log("📥 handle_offer_doc called")

    data = doc_snapshot.to_dict()
    if not data or data.get("type") != "offer":
        return

    offer_sdp = data.get("sdp")
    if not offer_sdp:
        return

    log("🚀 Creating answer now")

    asyncio.run_coroutine_threadsafe(
        handle_webrtc_offer(offer_sdp),
        webrtc_loop
    )

def webrtc_inbound_ice_listener(pc: RTCPeerConnection):
    def on_snapshot(col_snapshot, changes, _):
        for change in changes:
            if change.type.name != "ADDED":
                continue

            data = change.document.to_dict()
            if not data or "candidate" not in data:
                continue

            try:
                candidate = candidate_from_sdp(data["candidate"])
                candidate.sdpMid = data.get("sdpMid")
                candidate.sdpMLineIndex = data.get("sdpMLineIndex")

                asyncio.run_coroutine_threadsafe(
                    pc.addIceCandidate(candidate),
                    webrtc_loop
                )
            except Exception as e:
                log(f"ICE inbound error: {e}")

    pc_ref.collection("webrtc") \
        .document("candidates") \
        .collection("in") \
        .on_snapshot(on_snapshot)

# =====================================================
# MJPEG / COMMANDS / WATCHDOG
# =====================================================
def mjpeg_stream(profile="normal"):
    cfg = MJPEG_PROFILES.get(profile, MJPEG_PROFILES[DEFAULT_MJPEG_PROFILE])
    target_fps = cfg["fps"]
    quality = cfg["quality"]
    scale = cfg["scale"]

    frame_interval = 1.0 / max(1, target_fps)
    last_frame_time = 0

    with mss.mss() as sct:
        monitor = sct.monitors[1] if len(sct.monitors) > 1 else sct.monitors[0]

        while not stop_event.is_set():
            now = time.time()
            if now - last_frame_time < frame_interval:
                time.sleep(0.01)
                continue

            last_frame_time = now

            try:
                img = sct.grab(monitor)
                frame = Image.frombytes("RGB", img.size, img.rgb)

                if scale != 1.0:
                    w, h = frame.size
                    frame = frame.resize(
                        (int(w * scale), int(h * scale)),
                        Image.BILINEAR
                    )

                buf = io.BytesIO()
                frame.save(buf, format="JPEG", quality=quality)

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n"
                    + buf.getvalue() +
                    b"\r\n"
                )

            except Exception as e:
                log(f"MJPEG error: {e}")
            time.sleep(0.2)

@app.route("/mjpeg")
def mjpeg():
    mode = flask.request.args.get("mode", DEFAULT_MJPEG_PROFILE)
    log(f"MJPEG client connected from {flask.request.remote_addr} mode={mode}")
    return Response(
        mjpeg_stream(mode),
        mimetype="multipart/x-mixed-replace; boundary=frame"
    )

def run_mjpeg():
    while True:
        try:
            log("Starting MJPEG server")
            app.run(
                host="0.0.0.0",
                port=FLASK_PORT,
                threaded=True,
                debug=False,
                use_reloader=False
            )
        except Exception as e:
            log(f"MJPEG crashed: {e}, retrying in 10s")
            time.sleep(10)

def _show_message_dialog(text):
    global message_active
    with message_lock:
        message_active = True

    try:
        user32 = ctypes.windll.user32

        MB_OK = 0x00000000
        MB_ICONINFORMATION = 0x00000040
        MB_SYSTEMMODAL = 0x00001000
        MB_SETFOREGROUND = 0x00010000
        MB_TOPMOST = 0x00040000

        flags = (
            MB_OK
            | MB_ICONINFORMATION
            | MB_SYSTEMMODAL
            | MB_SETFOREGROUND
            | MB_TOPMOST
        )

        # Force foreground
        user32.AllowSetForegroundWindow(-1)

        user32.MessageBoxW(
            None,
            text,
            "Mainroad Pisonet Admin",
            flags
        )

    finally:
        with message_lock:
            message_active = False

def _force_close_message():
    try:
        hwnd = ctypes.windll.user32.FindWindowW(None, "Mainroad Pisonet Admin")
        if hwnd:
            ctypes.windll.user32.PostMessageW(hwnd, 0x0010, 0, 0)
    except:
        pass

def ensure_no_message_block():
    with message_lock:
        if message_active:
            _force_close_message()

def execute_command(cmd_type, payload):
    global session_seconds, session_active, last_tick
    cmd_type = (cmd_type or "").lower()

    log(f"Command received: {cmd_type}")

    if cmd_type != "message":
        ensure_no_message_block()

    if cmd_type == "lock":
        ctypes.windll.user32.LockWorkStation()

    elif cmd_type == "restart":
        cleanup_rtdb()
        end_session_if_active("restart")

        append_audit("SYSTEM_RESTART_INITIATED")

        # delay restart so Firestore update completes
        threading.Thread(
            target=lambda: (
                time.sleep(3),
                os.system("shutdown /r /t 0")
            ),
            daemon=True
        ).start()

    elif cmd_type == "shutdown":
        cleanup_rtdb()
        end_session_if_active("shutdown")

        append_audit("SYSTEM_SHUTDOWN_INITIATED")

        # delay shutdown so Firestore update completes
        threading.Thread(
            target=lambda: (
                time.sleep(3),
                os.system("shutdown /s /t 0")
            ),
            daemon=True
        ).start()

    elif cmd_type == "end_session":
        end_session_if_active("end_session")
        session_seconds = 0
        session_active = False
        last_tick = time.time()
        save_state()

    elif cmd_type == "message":
        threading.Thread(
            target=_show_message_dialog,
            args=(payload.get("text", ""),),
            daemon=True
        ).start()

def command_listener():
    backoff = 5

    def start_listener():
        nonlocal backoff

        def on_snapshot(col, changes, _):
            for change in changes:

                if change.type.name != "ADDED":
                    continue

                doc = change.document
                data = doc.to_dict() or {}

                cmd_type = data.get("type")
                payload = data.get("payload", {})
                status = data.get("status", "pending")

                # ✅ CRITICAL FIX: ignore executed commands
                if status != "pending":
                    append_audit("COMMAND_SKIPPED_ALREADY_EXECUTED", {
                        "docId": doc.id,
                        "status": status
                    })
                    continue

                append_audit("COMMAND_RECEIVED", {
                    "type": cmd_type,
                    "docId": doc.id
                })

                execute_command(cmd_type, payload)

                try:
                    # mark executed immediately
                    doc.reference.update({
                        "status": "executed",
                        "executedAt": firestore.SERVER_TIMESTAMP,
                        "deleteAfter": int(time.time()) + 60
                    })

                    append_audit("COMMAND_EXECUTED", {
                        "type": cmd_type,
                        "docId": doc.id
                    })

                except Exception as e:
                    append_audit("COMMAND_UPDATE_FAILED", {
                        "error": str(e),
                        "docId": doc.id
                    })

        try:
            append_audit("COMMAND_LISTENER_STARTED")

            pc_ref.collection("commands").on_snapshot(on_snapshot)

            backoff = 5

        except Exception as e:

            append_audit("COMMAND_LISTENER_CRASHED", {
                "error": str(e)
            })

            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
            start_listener()

    threading.Thread(target=start_listener, daemon=True).start()
    
def self_watchdog():
    while True:
        if heartbeat_thread and not heartbeat_thread.is_alive():
            log("Heartbeat thread died, exiting agent")
            os._exit(1)

        # MJPEG is NON-CRITICAL
        if mjpeg_thread and not mjpeg_thread.is_alive():
            log("MJPEG thread died, continuing without live view")

        time.sleep(30)

def webrtc_listener():
    def on_snapshot(doc_snapshots, changes, read_time):
        log("🔥 OFFER SNAPSHOT TRIGGERED")

        for doc in doc_snapshots:
            if not doc.exists:
                continue

            log("📄 Offer detected")
            handle_offer_doc(doc)

    pc_ref.collection("webrtc").document("offer").on_snapshot(on_snapshot)

def cleanup_old_commands():
    try:
        now_ts = int(time.time())

        docs = (
            pc_ref.collection("commands")
            .where("status", "==", "executed")   # 🔒 SAFETY FILTER
            .where("deleteAfter", "<", now_ts)
            .stream()
        )

        for d in docs:
            d.reference.delete()
            append_audit("CLEANUP_CMD_DELETED", {
                "docId": d.id
            })

    except Exception as e:
        append_audit("CLEANUP_FAILED", {
            "error": str(e)
        })
        
def cleanup_loop():
    while True:
        cleanup_old_commands()
        time.sleep(60)

def wait_for_network(timeout=40):
    t0 = time.time()

    while time.time() - t0 < timeout:
        try:
            # test internet connectivity
            urllib.request.urlopen("https://www.google.com", timeout=3)

            ip = get_local_ip()
            log(f"Internet ready: {ip}")
            return True

        except:
            log("Waiting for internet...")
            time.sleep(2)

    log("Internet not ready after wait")
    return False

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def main():
    global heartbeat_thread, mjpeg_thread
    global session_seconds, last_tick, last_end_reason
    global webrtc_loop

    wait_for_network(timeout=40)

    time.sleep(3)

    log("Verifying integrity...")
    verify_self_integrity()

    log("Checking for updates...")
    check_for_update()

    restore_audit_chain()

    log(f"Agent starting | PC_ID={PC_ID} | version={AGENT_VERSION}")

    load_state()

    if not current_session.get("id"):
        session_seconds = 0

    last_tick = time.time()
    last_end_reason = None
    save_state()

    # async loop
    webrtc_loop = asyncio.new_event_loop()
    threading.Thread(
        target=start_loop,
        args=(webrtc_loop,),
        daemon=True
    ).start()

    # background listeners (no audit writes yet)
    threading.Thread(target=webrtc_listener, daemon=True).start()
    threading.Thread(target=telemetry_loop, daemon=True).start()

    # MJPEG
    mjpeg_thread = threading.Thread(target=run_mjpeg, daemon=True)
    mjpeg_thread.start()

    threading.Thread(target=self_watchdog, daemon=True).start()

    # THIS writes first audit entry
    command_listener()

    time.sleep(1)

    # NOW integrity reflects real latest audit hash
    sync_integrity_now()

    # heartbeat LAST
    heartbeat_thread = threading.Thread(
        target=firebase_heartbeat,
        daemon=True
    )
    heartbeat_thread.start()

    threading.Thread(target=cleanup_loop, daemon=True).start()
    log("All systems started")

    while True:
        time.sleep(1)

if __name__ == "__main__":

    main()
