import json
from pathlib import Path

DEFAULTS = {
    "theme": "Dark",  # Dark | Light | System

    "ffmpeg_path": "",
    "ffprobe_path": "",

    "selected_preset": "Jazz24 (128k MP3)",
    "custom_url": "",

    "total_time_str": "1h 00m",
    "chunk_time_str": "15m",
    "fade_duration": "3",
    "filename_prefix": "STROAD_Rec",
    "output_path": str(Path.home() / "Downloads"),

    "output_format": "MP3 (encoded)"
}

def settings_path() -> Path:
    return Path.home() / ".stroad2.json"

def load_settings() -> dict:
    p = settings_path()
    if not p.exists():
        return dict(DEFAULTS)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULTS)
    merged = dict(DEFAULTS)
    if isinstance(data, dict):
        for k in DEFAULTS.keys():
            if k in data:
                merged[k] = data[k]
    return merged

def save_settings(values: dict) -> None:
    p = settings_path()
    merged = dict(DEFAULTS)
    for k in DEFAULTS.keys():
        merged[k] = values.get(k, merged[k])
    p.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
