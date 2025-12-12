# idea_server.py – v2.1 (16-ago-2025)
# - UN solo worker che usa la seriale
# - Scrittura immediata: NESSUNA CODA, vince sempre l’ULTIMO comando
# - Warm-up + flush iniziale
# - Lettura adattiva a chunk; gap RTU tra transazioni
# - Reopen dopo N errori consecutivi
# - Rilevazione "foreign frames" (function code inatteso) + read-back di salvataggio
# - FromiDea include metriche e flag FOREIGN_ALERT

import os, sys, time, csv, yaml, random, threading, collections
import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional
import xml.etree.ElementTree as ET

import minimalmodbus, serial

# ───── Percorsi
BASE_DIR  = Path.home() / "Desktop" / "test 2025-12-07"
FROM_FILE = BASE_DIR / "FromiDea.xml"
TO_FILE   = BASE_DIR / "ToiDea.xml"
CFG_FILE  = BASE_DIR / "ServeriDeaConfig.yaml"

# ───── Config
CFG_MTIME, CFG = 0, {}
CFG_LOCK = threading.Lock()
def load_config(force: bool = False):
    global CFG_MTIME, CFG
    try:
        mtime = CFG_FILE.stat().st_mtime
    except FileNotFoundError:
        print("[CFG] File mancante:", CFG_FILE)
        return
    if force or mtime != CFG_MTIME:
        with open(CFG_FILE, "r", encoding="utf-8") as fp:
            cfg = yaml.safe_load(fp) or {}
        with CFG_LOCK:
            CFG = cfg
            CFG_MTIME = mtime
        print("[CONFIG reload]", CFG)

load_config(force=True)
if CFG.get("autorun", 1) == 0 and not sys.stdin.isatty():
    print("autorun=0 → uscita")
    sys.exit(0)

# ───── Util
def uniq_sorted(addrs: Iterable[int]) -> List[int]:
    return sorted({int(a) for a in addrs})

def contiguous_blocks(addrs: List[int]) -> List[range]:
    if not addrs:
        return []
    a = uniq_sorted(addrs)
    blocks, start = [], a[0]
    for prev, cur in zip(a, a[1:]):
        if cur != prev + 1:
            blocks.append(range(start, prev + 1))
            start = cur
    blocks.append(range(start, a[-1] + 1))
    return blocks

def split_range(r: range, max_len: int) -> List[Tuple[int, int]]:
    out, s, e = [], r.start, r.stop
    while s < e:
        ln = min(max_len, e - s)
        out.append((s, ln))
        s += ln
    return out

def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

# ───── Stato & metriche
LAST_OK: Dict[int, int] = {}
STATS = dict(read_ok=0, read_err=0, write_ok=0, write_err=0, resyncs=0, last_reset_iso="")
LAST_WRITER_ID = ""

# Foreign-master detection
FOREIGN_TIMES = collections.deque(maxlen=100)
FOREIGN_FRAMES_TOTAL = 0
FOREIGN_ALERT = 0
FOREIGN_LAST_ISO = ""

def note_foreign():
    """Segnala frame 'di altri': function code inatteso ecc."""
    global FOREIGN_FRAMES_TOTAL, FOREIGN_ALERT, FOREIGN_LAST_ISO
    t = time.time()
    FOREIGN_TIMES.append(t)
    while FOREIGN_TIMES and t - FOREIGN_TIMES[0] > float(CFG.get("foreign_window_s", 10)):
        FOREIGN_TIMES.popleft()
    FOREIGN_FRAMES_TOTAL += 1
    FOREIGN_LAST_ISO = now_iso()
    FOREIGN_ALERT = 1 if len(FOREIGN_TIMES) >= int(CFG.get("foreign_threshold", 3)) else 0
    if FOREIGN_ALERT:
        print("[ALERT] Possibile secondo master sul bus (foreign frames in finestra).")

# ───── Write command (ULTIMO comando vince, nessuna coda)
WriteCmd = Dict[str, int]  # {"ID":str,"CMD":str,"IND":int,"VAL":int}
PENDING_CMD: Optional[WriteCmd] = None
CMD_LOCK = threading.Lock()

# ───── Bus RTU (unico punto che tocca la seriale)
class SerialBus:
    def __init__(self):
        self.instrument: Optional[minimalmodbus.Instrument] = None
        self.addr_corr: int = int(CFG.get("address_correction", 1))
        self.read_fcode: int = int(CFG.get("read_functioncode", 3))
        self.rtu_gap_ms: int = int(CFG.get("rtu_gap_ms", 5))

    def _apply(self):
        ins = self.instrument
        ins.serial.baudrate = int(CFG.get("baudrate", 38400))
        ins.serial.timeout  = float(CFG.get("serial_timeout_ms", 500)) / 1000.0
        ins.serial.write_timeout = max(1.0, ins.serial.timeout)
        ins.serial.parity   = {
            "NONE": serial.PARITY_NONE,
            "EVEN": serial.PARITY_EVEN,
            "ODD":  serial.PARITY_ODD
        }[str(CFG.get("parity", "NONE")).upper()]
        ins.serial.stopbits = int(CFG.get("stopbits", 1))
        ibt = float(CFG.get("inter_byte_timeout_ms", 10)) / 1000.0
        if ibt > 0:
            ins.serial.inter_byte_timeout = ibt
        ins.clear_buffers_before_each_transaction = True
        ins.close_port_after_each_call = False
        ins.handle_local_echo = bool(CFG.get("handle_local_echo", 0))

    def open(self):
        self.instrument = minimalmodbus.Instrument(
            CFG["serial_port"],
            int(CFG.get("slave_id", 1)),
            mode=minimalmodbus.MODE_RTU,
        )
        self._apply()
        try:
            self.instrument.serial.open()
        except Exception:
            pass
        self._warmup()

    def _flush(self):
        try:
            self.instrument.serial.reset_input_buffer()
            self.instrument.serial.reset_output_buffer()
        except Exception:
            try:
                self.instrument.serial.flushInput()
                self.instrument.serial.flushOutput()
            except Exception:
                pass

    def _warmup(self):
        time.sleep(float(CFG.get("startup_warmup_ms", 700)) / 1000.0)
        self._flush()
        addrs = CFG.get("read_blocks", [])
        if addrs:
            first_yaml = int(addrs[0])
            wire_0b = (first_yaml - 1) + self.addr_corr
            for _ in range(int(CFG.get("startup_dummy_reads", 3))):
                try:
                    self.instrument.read_registers(
                        wire_0b,
                        1,
                        functioncode=self.read_fcode
                    )
                except Exception:
                    pass
                time.sleep(float(CFG.get("retry_backoff_ms", 80)) / 1000.0)
        self._flush()

    def reopen(self):
        try:
            if self.instrument:
                self.instrument.serial.close()
        except Exception:
            pass
        self.open()
        STATS["resyncs"] += 1
        STATS["last_reset_iso"] = now_iso()

    def _gap(self):
        time.sleep(self.rtu_gap_ms / 1000.0)

    def read_regs(self, yaml_start: int, qty: int) -> Optional[List[int]]:
        wire_0b = (yaml_start - 1) + self.addr_corr
        regs = self.instrument.read_registers(
            wire_0b,
            qty,
            functioncode=self.read_fcode
        )
        self._gap()
        return regs

    def write_reg(self, yaml_addr: int, val: int):
        wire_0b = (yaml_addr - 1) + self.addr_corr
        self.instrument.write_register(
            wire_0b,
            val,
            number_of_decimals=0,
            functioncode=6,
            signed=False
        )
        self._gap()

BUS = SerialBus()
BUS.open()

# ───── CSV
CSV_FILE = WRITER = CSV_START = None
def csv_open():
    if not CFG.get("csv_enable", True):
        return
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    global CSV_FILE, WRITER, CSV_START
    CSV_START = dt.datetime.now()
    CSV_FILE  = (BASE_DIR / f"logfile_{CSV_START:%Y%m%d_%H%M%S}_.csv").open(
        "w", newline="", encoding="utf-8"
    )
    WRITER    = csv.writer(CSV_FILE)
    WRITER.writerow(["DATA"] + list(CFG.get("read_blocks", [])))
    print("[LOG] start", CSV_FILE.name)

def csv_rollover():
    if not CFG.get("csv_enable", True):
        return
    if CSV_FILE and CSV_FILE.tell() >= int(CFG.get("log_rollover_bytes", 5_000_000)):
        end = dt.datetime.now()
        final = BASE_DIR / f"logfile_{CSV_START:%Y%m%d_%H%M%S}_to_{end:%Y%m%d_%H%M%S}.csv"
        try:
            CSV_FILE.close()
            CSV_FILE.rename(final)
        finally:
            csv_open()

csv_open()

# ───── FromiDea.xml
def write_fromidea(values: Dict[int, int], any_ok: bool):
    now = dt.datetime.now()
    root = ET.Element("rootmain")
    ET.SubElement(root, "DATA").text = now.strftime("%d/%m/%Y %H:%M:%S")
    if bool(CFG.get("from_include_meta", True)):
        ET.SubElement(root, "BUS_OK").text = "1" if any_ok else "0"
        ET.SubElement(root, "TS_ISO").text = now_iso()
        ET.SubElement(root, "READ_OK_TOTAL").text  = str(STATS["read_ok"])
        ET.SubElement(root, "READ_ERR_TOTAL").text = str(STATS["read_err"])
        ET.SubElement(root, "WRITE_OK_TOTAL").text = str(STATS["write_ok"])
        ET.SubElement(root, "WRITE_ERR_TOTAL").text= str(STATS["write_err"])
        ET.SubElement(root, "RESYNCS").text        = str(STATS["resyncs"])
        ET.SubElement(root, "LAST_RESET_ISO").text = STATS["last_reset_iso"]
        ET.SubElement(root, "WRITER_LAST_ID").text = LAST_WRITER_ID or ""
        ET.SubElement(root, "FOREIGN_FRAMES_TOTAL").text = str(FOREIGN_FRAMES_TOTAL)
        ET.SubElement(root, "FOREIGN_ALERT").text        = "1" if FOREIGN_ALERT else "0"
        ET.SubElement(root, "FOREIGN_LAST_ISO").text     = FOREIGN_LAST_ISO or ""
    for a in CFG.get("read_blocks", []):
        ET.SubElement(root, str(a)).text = str(values.get(a, LAST_OK.get(a, 0)))
    tmp = FROM_FILE.with_suffix(".tmp")
    tree = ET.ElementTree(root)
    try:
        tree.write(tmp, "utf-8", xml_declaration=True)
        os.replace(tmp, FROM_FILE)
    except PermissionError as e:
        if bool(CFG.get("xml_ignore_errors", False)):
            print(f"[WARN] FromiDea in uso: salto → {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        else:
            raise
    except Exception as e:
        print("[ERR] XML write:", e)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

# ───── Worker unico (I/O)
BUS_READY = threading.Event()

def io_worker():
    global LAST_WRITER_ID, PENDING_CMD
    first_ok = False

    while True:
        try:
            load_config()

            # 1) SCRITTURA IMMEDIATA: prendo solo l'ULTIMO comando disponibile (nessuna coda)
            cmd = None
            with CMD_LOCK:
                if PENDING_CMD is not None:
                    cmd = PENDING_CMD
                    PENDING_CMD = None

            if cmd is not None:
                verify     = bool(CFG.get("verify_writes", 1))
                w_retries  = int(CFG.get("write_retries", 3))
                commit_reg = int(CFG.get("writer_commit_register", 0))
                commit_val = int(CFG.get("writer_commit_value", 1))

                attempts, ok = 0, False
                while attempts < w_retries and not ok:
                    attempts += 1
                    try:
                        BUS.write_reg(cmd["IND"], cmd["VAL"])
                        if cmd["CMD"] == "09" and commit_reg > 0:
                            BUS.write_reg(commit_reg, commit_val)
                        ok = True
                    except Exception as e:
                        msg = str(e)
                        print("[WRITER ERR]", msg)
                        if "Wrong functioncode" in msg or "functioncode" in msg:
                            note_foreign()
                        if verify:
                            try:
                                rb = BUS.read_regs(cmd["IND"], 1)
                                if rb is not None and int(rb[0]) == int(cmd["VAL"]):
                                    ok = True
                                    print(f"[WRITER] conferma via read-back IND={cmd['IND']}=VAL={cmd['VAL']}")
                                else:
                                    time.sleep(float(CFG.get("retry_backoff_ms", 80))/1000.0)
                            except Exception as e2:
                                print("[WRITER WARN] read-back fallito:", e2)
                                time.sleep(float(CFG.get("retry_backoff_ms", 80))/1000.0)
                        else:
                            time.sleep(float(CFG.get("retry_backoff_ms", 80))/1000.0)

                if ok:
                    LAST_WRITER_ID = str(cmd["ID"])
                    STATS["write_ok"] += 1
                    print(f"[WRITER] eseguito ID={cmd['ID']} IND={cmd['IND']} VAL={cmd['VAL']}")
                else:
                    STATS["write_err"] += 1
                    print(f"[WRITER ERR] non eseguito ID={cmd['ID']} dopo {w_retries} tentativi")

            # 2) LETTURE
            addrs = uniq_sorted(CFG.get("read_blocks", []))
            if not addrs:
                time.sleep(0.2)
                continue

            max_chunk = int(CFG.get("read_max_chunk", 4))
            min_chunk = int(CFG.get("read_min_chunk", 1))
            max_retry = int(CFG.get("max_retry", 4))
            back_ms   = int(CFG.get("retry_backoff_ms", 80))

            values: Dict[int, int] = {}
            any_ok = False

            for block in contiguous_blocks(addrs):
                for s, ln in split_range(block, max_chunk):
                    cur = ln
                    while cur >= min_chunk:
                        regs = None
                        for _ in range(max_retry):
                            try:
                                regs = BUS.read_regs(s, cur)
                                break
                            except minimalmodbus.NoResponseError:
                                time.sleep(back_ms / 1000.0)
                            except Exception as e:
                                msg = str(e)
                                print("[BUS ERR]", msg)
                                if "Wrong functioncode" in msg or "functioncode" in msg:
                                    note_foreign()
                                time.sleep(back_ms / 1000.0)
                        if regs is None:
                            STATS["read_err"] += 1
                            cur //= 2
                            if cur < min_chunk:
                                print(f"[BUS] no answer blocco {s}-{s+ln-1}")
                            continue
                        STATS["read_ok"] += 1
                        for off, v in enumerate(regs):
                            addr = s + off
                            iv = int(v)
                            values[addr] = iv
                            LAST_OK[addr] = iv
                        any_ok = True
                        break

            write_fromidea(values, any_ok)

            if CFG.get("csv_enable", True):
                now = dt.datetime.now().isoformat(sep=" ", timespec="seconds")
                WRITER.writerow([now] + [LAST_OK.get(a, "") for a in addrs])
                csv_rollover()

            if any_ok and not first_ok:
                BUS_READY.set()
                first_ok = True

        except serial.SerialException as e:
            print("[BUS] SerialException:", e)
            BUS.reopen()
        except Exception as e:
            print("[ERR worker]", e)

        base = int(CFG.get("poll_ms", 500))
        jitter = int(CFG.get("poll_jitter_ms", 20))
        time.sleep(max(0, (base + random.randint(-jitter, +jitter))) / 1000.0)

# ───── Watcher ToiDea -> comando immediato (ULTIMO vince)
LAST_TOIDEA_ID = ""

def toidea_watcher():
    global LAST_TOIDEA_ID, PENDING_CMD
    while True:
        try:
            if not TO_FILE.exists():
                time.sleep(float(CFG.get("writer_sleep_ms", 200)) / 1000.0)
                continue

            root = ET.parse(TO_FILE).getroot()
            cid = (root.findtext("ID") or "").strip()
            if not cid or cid == LAST_TOIDEA_ID:
                time.sleep(float(CFG.get("writer_sleep_ms", 200)) / 1000.0)
                continue

            cmd = (root.findtext("CMD") or "").strip()
            ind = int(root.findtext("IND", 0))
            val = int(root.findtext("VAL", 0))

            if cmd not in ("07", "09"):
                print("[WRITER] CMD non valido:", cmd)
                LAST_TOIDEA_ID = cid
                continue

            # Imposta SOLO l'ULTIMO comando disponibile; quelli vecchi non ancora eseguiti vengono sovrascritti
            with CMD_LOCK:
                PENDING_CMD = {"ID": cid, "CMD": cmd, "IND": ind, "VAL": val}

            LAST_TOIDEA_ID = cid

        except Exception as e:
            print("[WRITER WATCH ERR]", e)
            time.sleep(float(CFG.get("writer_sleep_ms", 200)) / 1000.0)

# ───── main
if __name__ == "__main__":
    threading.Thread(target=io_worker, daemon=True).start()
    threading.Thread(target=toidea_watcher, daemon=True).start()
    print("Server iDea avviato – Ctrl-C per uscire")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Bye")
