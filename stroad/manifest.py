import json
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any


def _atomic_write_json(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


class SessionManifest:
    def __init__(
        self,
        out_dir: str,
        session_id: str,
        app_name: str,
        app_version: str,
        station_url: str,
        preset_name: str,
        short_code: Optional[str],
        chunk_seconds: int,
        tape_mode: bool,
        output_format: str,
    ):
        self._lock = threading.Lock()
        self.path = Path(out_dir) / f"STROAD_Rec_{session_id}.session.json"
        self.data = {
            "manifest_version": 1,
            "app": {"name": app_name, "version": app_version},
            "session": {
                "id": session_id,
                "start_local": self._now_local(),
                "end_local": None,
                "status": "recording",
            },
            "station": {
                "url": station_url,
                "short_code": short_code,
                "preset_name": preset_name,
                "format": output_format,
            },
            "settings": {
                "chunk_seconds": chunk_seconds,
                "tape_mode": tape_mode,
                "output_dir": str(Path(out_dir)),
            },
            "events": [{"t": self._now_local(), "type": "session_start"}],
            "chunks": [],
            "errors": [],
        }
        with self._lock:
            _atomic_write_json(self.path, self.data)

    def _now_local(self) -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")

    def event(self, typ: str, **extra) -> None:
        with self._lock:
            e = {"t": self._now_local(), "type": typ}
            e.update(extra)
            self.data["events"].append(e)
            _atomic_write_json(self.path, self.data)

    def add_chunk(
        self,
        index: int,
        start_local: str,
        end_local: str,
        planned_seconds: int,
        actual_seconds: float,
        output_file: str,
        bytes_written: int,
        ffmpeg_exit_code: int,
    ) -> None:
        with self._lock:
            self.data["chunks"].append(
                {
                    "index": index,
                    "start_local": start_local,
                    "end_local": end_local,
                    "planned_seconds": planned_seconds,
                    "actual_seconds": actual_seconds,
                    "output_file": output_file,
                    "bytes": bytes_written,
                    "ffmpeg_exit_code": ffmpeg_exit_code,
                }
            )
            _atomic_write_json(self.path, self.data)

    def error(
        self,
        message: str,
        exit_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            item: Dict[str, Any] = {
                "t": self._now_local(),
                "message": message,
                "exit_code": exit_code,
            }
            if details:
                item["details"] = details
            self.data["errors"].append(item)
            _atomic_write_json(self.path, self.data)

    def finalize(self, status: str) -> None:
        with self._lock:
            self.data["session"]["end_local"] = self._now_local()
            self.data["session"]["status"] = status
            self.data["events"].append(
                {"t": self._now_local(), "type": "session_end", "status": status}
            )
            _atomic_write_json(self.path, self.data)
