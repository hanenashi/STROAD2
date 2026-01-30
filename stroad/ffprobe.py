import os
import json
import subprocess
import re

def ffprobe_tags(ffprobe_path: str, stream_url: str, timeout: int = 6) -> dict:
    ffprobe = (ffprobe_path or "").strip()
    if not ffprobe or not os.path.exists(ffprobe):
        return {}
    cmd = [
        ffprobe,
        "-v", "error",
        "-print_format", "json",
        "-show_entries", "format_tags",
        stream_url
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout)
        data = json.loads(out)
        tags = (data.get("format", {}) or {}).get("tags", {}) or {}
        norm = {}
        for k, v in tags.items():
            if isinstance(v, str):
                norm[k.strip().lower()] = v.strip()
        return norm
    except Exception:
        return {}

def station_name_from_tags(tags: dict, selected_preset: str) -> str:
    name = (tags.get("icy-name") or tags.get("icy_name") or "").strip()
    if name:
        return name
    preset = (selected_preset or "").strip()
    if preset and preset.lower() != "custom url":
        return preset
    return "STROAD Radio"

_STOPWORDS = {"radio","fm","stream","live","official","somafm","bbc","the","a","an"}

def station_short_code(station_name: str, preset_name: str = "") -> str:
    # Prefer a cleaned preset first if it contains a strong identifier (Jazz24, etc.)
    basis = (preset_name or "").strip() or (station_name or "").strip() or "STROAD"
    # Remove parenthetical info
    basis = re.sub(r"\(.*?\)", "", basis)
    # Split into words, strip noise
    words = [re.sub(r"[^A-Za-z0-9]+", "", w) for w in basis.split()]
    words = [w for w in words if w and w.lower() not in _STOPWORDS]
    if not words:
        return "STROAD"
    # Heuristic: if first word contains digits, keep it; else take first word only.
    w0 = words[0]
    code = w0
    # If it's too generic, maybe take second word
    if len(code) < 4 and len(words) > 1:
        code = (words[0] + words[1])
    code = code.upper()
    return code[:12]
