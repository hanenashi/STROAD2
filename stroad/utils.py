import re
import time
import datetime

def parse_time_string(time_str: str) -> int:
    """
    Robustly parses human time strings.
    Examples: '1h 30m', '90', '90s', '1h 20m 30s', '  10m  '
    """
    s = (time_str or "").strip().lower()
    if not s:
        return 0
    
    # If it's just a number (e.g. "90"), treat as seconds
    if s.isdigit():
        return int(s)
        
    total_seconds = 0
    found_any = False
    
    # Find all pairs of numbers+units
    # Matches: "1h", " 20 m ", "30s"
    parts = re.findall(r'(\d+)\s*([hms])', s)
    
    for value, unit in parts:
        found_any = True
        v = int(value)
        if unit == 'h':
            total_seconds += v * 3600
        elif unit == 'm':
            total_seconds += v * 60
        elif unit == 's':
            total_seconds += v
            
    # Fallback: if user typed "90.5" or some other number format regex missed
    if not found_any:
        try:
            return int(float(s))
        except ValueError:
            return 0
            
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