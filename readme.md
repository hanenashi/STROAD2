# STROAD 2.1.0

Stream recorder with chunking + fade + tagging, now with:

- **Session manifest**: writes `STROAD_Rec_YYYYMMDD_HHMMSS.session.json` next to the output files.
- **Preferences**: set defaults (paths, timing, output format) + theme (Dark / Light / System) persisted to `~/.stroad2.json`.
- Code split into modules under `stroad/`.

## Run

```bash
python stroad2.py
```

## Notes

- Manifest is written only at safe boundaries (session start, chunk complete, session end).
- "System" theme keeps ttk defaults; log window becomes plain white/black for readability.
