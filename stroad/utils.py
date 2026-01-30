import re
import time
import datetime

def parse_time_string(time_str: str) -> int:
    time_str = (time_str or "").strip().lower()
    total_seconds = 0
    parts = re.findall(r'(\d+)\s*([hms])', time_str)
    if not parts and time_str.isdigit():
        return int(time_str)
    for value, unit in parts:
        v = int(value)
        if unit == 'h':
            total_seconds += v * 3600
        elif unit == 'm':
            total_seconds += v * 60
        elif unit == 's':
            total_seconds += v
    return total_seconds

def safe_int(s, default=0):
    try:
        return int(str(s).strip())
    except Exception:
        return default

def fmt_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"

def fmt_title_range(start_dt: datetime.datetime, dur_seconds: int) -> str:
    end_dt = start_dt + datetime.timedelta(seconds=int(dur_seconds))
    if dur_seconds < 600:
        return f"{start_dt:%Y-%m-%d %H:%M:%S}–{end_dt:%H:%M:%S}"
    return f"{start_dt:%Y-%m-%d %H:%M}–{end_dt:%H:%M}"

def log_line(msg: str) -> str:
    ts = time.strftime("%H:%M:%S")
    return f"[{ts}] {msg}"
