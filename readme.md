# STROAD 2.0

A small Tkinter GUI that records internet radio streams with FFmpeg, saves them in timed chunks, and applies smooth fade-in/out so even tiny gaps don’t sound nasty.

## Features
- Presets + custom URL
- Record by **total time** and **chunk length**
- Real-time capture (`-re`) to avoid “buffer dump / repeated audio”
- Background processing pipeline (capture doesn’t wait for encoding)
- Output: **MP3** (libmp3lame) or **M4A/AAC**
- Tags per chunk:
  - `album` = station name (best-effort via ffprobe / preset fallback)
  - `artist` = your prefix (default `STROAD_Rec`)
  - `title` = timestamp range (e.g. `2026-01-30 15:28:46–15:29:16`)
- Clean dark/gray UI + log window

## Requirements
- Python 3.x
- FFmpeg installed and in PATH (or set path in the GUI)
- (Optional) ffprobe (usually comes with ffmpeg) for better station naming

## Run
```bash
python stroad2.py

