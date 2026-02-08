import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import subprocess
import threading
import os
import time
import datetime
import queue
import uuid
import json
import shutil
from typing import Tuple, List, Deque
from collections import deque

from .constants import APP_TITLE, APP_NAME, APP_VERSION
from .settings import load_settings, save_settings
from .themes import apply_theme, THEMES
from .utils import parse_time_string, safe_int, fmt_mmss, fmt_title_range, log_line
from .ffprobe import ffprobe_tags, station_name_from_tags, station_short_code
from .manifest import SessionManifest


class StroadApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("680x820")

        # --- STREAMS (JSON) ---
        self.streams_path = "streams.json"
        self.presets = self.load_streams()

        # Load persisted settings
        self.cfg = load_settings()

        # --- Variables ---
        self.ffmpeg_path = tk.StringVar(value=self.cfg.get("ffmpeg_path") or self.find_bin("ffmpeg"))
        self.ffprobe_path = tk.StringVar(value=self.cfg.get("ffprobe_path") or self.find_bin("ffprobe"))
        self.ffplay_path = tk.StringVar(value=self.cfg.get("ffplay_path") or self.find_bin("ffplay"))

        self.selected_preset = tk.StringVar(value=self.cfg.get("selected_preset") or list(self.presets.keys())[0])
        
        # Ensure selected preset actually exists
        if self.selected_preset.get() not in self.presets and self.selected_preset.get() != "Custom URL":
            self.selected_preset.set(list(self.presets.keys())[0])

        default_url = self.presets.get(self.selected_preset.get(), "")
        if self.selected_preset.get() == "Custom URL":
            default_url = self.cfg.get("custom_url", "") or ""
        self.url = tk.StringVar(value=default_url)

        self.total_time_str = tk.StringVar(value=self.cfg.get("total_time_str", "1h 00m"))
        self.chunk_time_str = tk.StringVar(value=self.cfg.get("chunk_time_str", "15m"))
        self.fade_duration = tk.StringVar(value=self.cfg.get("fade_duration", "3"))
        self.filename_prefix = tk.StringVar(value=self.cfg.get("filename_prefix", "STROAD_Rec"))
        self.output_path = tk.StringVar(value=self.cfg.get("output_path") or os.path.expanduser("~/Downloads"))

        self.output_format = tk.StringVar(value=self.cfg.get("output_format", "MP3 (encoded)"))
        self.output_format_options = ["MP3 (encoded)", "M4A (AAC encoded)"]

        self.theme_name = tk.StringVar(value=self.cfg.get("theme", "Dark"))

        # --- Runtime state ---
        self.is_running = False
        self.stop_requested = False
        self.current_process = None
        self.manifest = None
        self.session_id = None
        self.play_process = None

        # Track outcome
        self._chunks_ok = 0
        self._chunks_fail = 0
        self._user_stopped = False

        # Thread-safe UI logging
        self.log_q = queue.Queue()
        self.job_q = queue.Queue()
        self.capture_thread = None
        self.process_thread = None

        # UI vars
        self.status_text = tk.StringVar(value="Idle.")
        self.chunk_progress_text = tk.StringVar(value="Chunk: -/-")
        self.time_progress_text = tk.StringVar(value="Time: 00:00 / 00:00")

        self.palette = apply_theme(self.root, self.theme_name.get())

        self.build_ui()
        self.root.after(80, self._pump_log_queue)

    # -------------------- Stream Management --------------------
    def load_streams(self):
        defaults = {
            "Jazz24 (128k MP3)": "https://knkx-live-a.edge.audiocdn.com/6285_128k",
            "Jazz24 (256k AAC)": "https://knkx-live-a.edge.audiocdn.com/6285_256k",
            "BBC Radio 1 (HLS)": "http://as-hls-ww-live.akamaized.net/pool_904/live/ww/bbc_radio_one/bbc_radio_one.isml/bbc_radio_one-audio=96000.norewind.m3u8",
            "SomaFM Groove Salad": "http://ice1.somafm.com/groovesalad-128-mp3",
            "Custom URL": ""
        }
        if os.path.exists(self.streams_path):
            try:
                with open(self.streams_path, 'r') as f:
                    loaded = json.load(f)
                    if "Custom URL" not in loaded: loaded["Custom URL"] = ""
                    return loaded
            except Exception: pass
        return defaults

    def save_streams(self):
        with open(self.streams_path, 'w') as f:
            json.dump(self.presets, f, indent=4)

    def open_stream_editor(self):
        win = tk.Toplevel(self.root)
        win.title("Manage Streams")
        win.geometry("400x350")
        
        list_frame = ttk.Frame(win)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        lb = tk.Listbox(list_frame, bg=self.palette["field"], fg=self.palette["text"])
        lb.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(list_frame, command=lb.yview)
        scroll.pack(side="right", fill="y")
        lb.config(yscrollcommand=scroll.set)

        for name in self.presets:
            if name != "Custom URL": lb.insert("end", name)

        entry_frame = ttk.Frame(win)
        entry_frame.pack(fill="x", padx=10, pady=5)
        name_var = tk.StringVar()
        url_var = tk.StringVar()
        ttk.Label(entry_frame, text="Name:").grid(row=0, column=0, sticky="w")
        ttk.Entry(entry_frame, textvariable=name_var).grid(row=0, column=1, sticky="ew")
        ttk.Label(entry_frame, text="URL:").grid(row=1, column=0, sticky="w")
        ttk.Entry(entry_frame, textvariable=url_var).grid(row=1, column=1, sticky="ew")
        entry_frame.columnconfigure(1, weight=1)

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=10, pady=10)

        def add_item():
            n, u = name_var.get().strip(), url_var.get().strip()
            if n and u:
                self.presets[n] = u
                self.save_streams()
                refresh_list()
                self.update_combobox()
                name_var.set("")
                url_var.set("")

        def delete_item():
            sel = lb.curselection()
            if sel:
                name = lb.get(sel[0])
                if name in self.presets:
                    del self.presets[name]
                    self.save_streams()
                    refresh_list()
                    self.update_combobox()

        def refresh_list():
            lb.delete(0, "end")
            for name in self.presets:
                if name != "Custom URL": lb.insert("end", name)

        ttk.Button(btn_frame, text="Add/Update", command=add_item).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Delete Selected", command=delete_item).pack(side="right", padx=5)

    def update_combobox(self):
        self.preset_combo['values'] = list(self.presets.keys())

    # -------------------- Theme & settings --------------------
    def persist_defaults_from_ui(self):
        values = {
            "theme": self.theme_name.get(),
            "ffmpeg_path": self.ffmpeg_path.get().strip(),
            "ffprobe_path": self.ffprobe_path.get().strip(),
            "ffplay_path": self.ffplay_path.get().strip(),
            "selected_preset": self.selected_preset.get(),
            "custom_url": self.url.get().strip() if self.selected_preset.get() == "Custom URL" else self.cfg.get("custom_url", ""),
            "total_time_str": self.total_time_str.get(),
            "chunk_time_str": self.chunk_time_str.get(),
            "fade_duration": self.fade_duration.get(),
            "filename_prefix": self.filename_prefix.get(),
            "output_path": self.output_path.get(),
            "output_format": self.output_format.get(),
        }
        save_settings(values)
        self.cfg = load_settings()

    def open_preferences(self):
        win = tk.Toplevel(self.root)
        win.title("Preferences")
        win.geometry("520x550")
        win.transient(self.root)
        win.grab_set()

        frm = ttk.Frame(win, padding=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Theme:").grid(row=0, column=0, sticky="w", pady=6)
        ttk.Combobox(frm, textvariable=self.theme_name, values=list(THEMES.keys()), state="readonly", width=18).grid(row=0, column=1, sticky="w", pady=6)
        
        def _apply_theme_now():
            self.palette = apply_theme(self.root, self.theme_name.get())
            self._apply_log_colors()
        ttk.Button(frm, text="Apply Theme", command=_apply_theme_now).grid(row=0, column=2, padx=8, pady=6)

        sep = ttk.Separator(frm, orient="horizontal")
        sep.grid(row=1, column=0, columnspan=3, sticky="ew", pady=10)

        # Paths
        for i, (lbl, var) in enumerate([("FFmpeg Path:", self.ffmpeg_path), 
                                        ("FFprobe Path:", self.ffprobe_path), 
                                        ("FFplay Path:", self.ffplay_path)], start=3):
            ttk.Label(frm, text=lbl).grid(row=i, column=0, sticky="w", pady=4)
            ttk.Entry(frm, textvariable=var).grid(row=i, column=1, sticky="ew", pady=4)
            ttk.Button(frm, text="...", width=3, command=lambda v=var: self._pick_file(v)).grid(row=i, column=2, padx=6)

        ttk.Label(frm, text="Default output folder:").grid(row=6, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.output_path).grid(row=6, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="...", width=3, command=lambda: self._pick_dir(self.output_path)).grid(row=6, column=2, padx=6)

        btns = ttk.Frame(frm)
        btns.grid(row=10, column=0, columnspan=3, sticky="e", pady=16)
        ttk.Button(btns, text="Save Defaults", command=lambda: [self.persist_defaults_from_ui(), messagebox.showinfo("Saved", "Settings saved.")]).pack(side="right", padx=6)
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
        frm.columnconfigure(1, weight=1)

    def _pick_file(self, var):
        picked = filedialog.askopenfilename()
        if picked: var.set(picked)

    def _pick_dir(self, var):
        picked = filedialog.askdirectory()
        if picked: var.set(picked)

    def find_bin(self, name: str) -> str:
        return shutil.which(name) or ""

    def log(self, msg: str):
        self.log_q.put(log_line(msg))

    def _pump_log_queue(self):
        try:
            while True:
                line = self.log_q.get_nowait()
                self.log_area.insert("end", f"> {line}\n")
                self.log_area.see("end")
        except queue.Empty: pass
        self.root.after(80, self._pump_log_queue)

    # -------------------- UI --------------------
    def on_preset_change(self, event=None):
        choice = self.selected_preset.get()
        if choice != "Custom URL": self.url.set(self.presets.get(choice, ""))

    def build_ui(self):
        # 1. Configuration
        f_conf = ttk.LabelFrame(self.root, text="Configuration")
        f_conf.pack(fill="x", padx=10, pady=6)
        
        stream_row = ttk.Frame(f_conf)
        stream_row.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        self.preset_combo = ttk.Combobox(stream_row, textvariable=self.selected_preset, values=list(self.presets.keys()), state="readonly")
        self.preset_combo.pack(side="left", fill="x", expand=True)
        self.preset_combo.bind("<<ComboboxSelected>>", self.on_preset_change)
        ttk.Button(stream_row, text="Manage", command=self.open_stream_editor, width=8).pack(side="left", padx=(5,0))
        ttk.Label(f_conf, text="Stream:").grid(row=0, column=0, sticky="w", padx=5)

        ttk.Label(f_conf, text="URL:").grid(row=1, column=0, sticky="w", padx=5)
        ttk.Entry(f_conf, textvariable=self.url).grid(row=1, column=1, sticky="ew", padx=5, pady=5)
        self._add_row(f_conf, 2, "Save To:", self.output_path, browse_dir=True)
        
        ttk.Label(f_conf, text="Output:").grid(row=3, column=0, sticky="w", padx=5)
        ttk.Combobox(f_conf, textvariable=self.output_format, values=self.output_format_options, state="readonly").grid(row=3, column=1, sticky="ew", padx=5, pady=5)
        ttk.Button(f_conf, text="Preferences…", command=self.open_preferences).grid(row=4, column=1, sticky="e", padx=5, pady=(8, 5))
        f_conf.columnconfigure(1, weight=1)

        # 2. Timing
        f_time = ttk.LabelFrame(self.root, text="Timing & Metadata")
        f_time.pack(fill="x", padx=10, pady=6)
        self._add_row(f_time, 0, "Total Time:", self.total_time_str)
        self._add_row(f_time, 1, "Chunk Length:", self.chunk_time_str)
        
        f_details = ttk.Frame(f_time)
        f_details.grid(row=2, column=0, columnspan=3, sticky="ew", padx=5, pady=6)
        ttk.Label(f_details, text="Prefix:").pack(side="left")
        ttk.Entry(f_details, textvariable=self.filename_prefix, width=18).pack(side="left", padx=5)
        ttk.Label(f_details, text="Fade (s):").pack(side="left")
        ttk.Entry(f_details, textvariable=self.fade_duration, width=6).pack(side="left", padx=5)

        # 3. Actions
        f_act = ttk.Frame(self.root)
        f_act.pack(fill="x", padx=10, pady=8)
        self.btn_play = ttk.Button(f_act, text="▶ PLAY STREAM", command=self.toggle_play)
        self.btn_play.pack(side="left", fill="x", expand=True, padx=5)
        self.btn_start = ttk.Button(f_act, text="🔴 RECORD", command=self.start_process)
        self.btn_start.pack(side="left", fill="x", expand=True, padx=5)
        self.btn_stop = ttk.Button(f_act, text="⏹ STOP & FADE", command=self.stop_process, state="disabled")
        self.btn_stop.pack(side="right", padx=5)

        # 4. Progress
        f_prog = ttk.LabelFrame(self.root, text="Progress")
        f_prog.pack(fill="x", padx=10, pady=6)
        ttk.Label(f_prog, textvariable=self.status_text).pack(anchor="w", padx=8, pady=2)
        ttk.Label(f_prog, textvariable=self.chunk_progress_text).pack(anchor="w", padx=8, pady=2)
        ttk.Label(f_prog, textvariable=self.time_progress_text).pack(anchor="w", padx=8, pady=2)
        self.pb_chunk = ttk.Progressbar(f_prog, orient="horizontal", mode="determinate", maximum=100)
        self.pb_chunk.pack(fill="x", padx=8, pady=6)
        self.pb_total = ttk.Progressbar(f_prog, orient="horizontal", mode="determinate", maximum=100)
        self.pb_total.pack(fill="x", padx=8, pady=(0, 8))

        # 5. Log
        self.log_area = scrolledtext.ScrolledText(self.root, height=16, bg=self.palette["bg"], fg=self.palette["accent"], insertbackground=self.palette["text"], font=("Menlo", 11))
        self.log_area.pack(fill="both", expand=True, padx=10, pady=6)
        self._apply_log_colors()

    def _apply_log_colors(self):
        if self.theme_name.get() == "System":
            self.log_area.configure(bg="white", fg="black", insertbackground="black")
        else:
            self.log_area.configure(bg=self.palette["bg"], fg=self.palette["accent"], insertbackground=self.palette["text"])

    def _add_row(self, parent, row, label, var, browse=False, browse_dir=False):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=5, pady=5)
        ttk.Entry(parent, textvariable=var).grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        if browse or browse_dir:
            cmd = lambda: var.set(filedialog.askdirectory() if browse_dir else filedialog.askopenfilename() or var.get())
            ttk.Button(parent, text="...", width=3, command=cmd).grid(row=row, column=2, padx=5)

    # -------------------- PLAYBACK --------------------
    def toggle_play(self):
        if self.play_process: self.stop_playback()
        else: self.start_playback()

    def start_playback(self):
        url = self.url.get().strip()
        ffplay = self.ffplay_path.get().strip()
        if not url: return messagebox.showerror("Error", "No URL to play!")
        if not ffplay or not os.path.exists(ffplay): return messagebox.showerror("Error", "FFplay not found. Check Preferences.")
        self.log(f"PLAYBACK: Starting stream {url}")
        cmd = [ffplay, "-nodisp", "-autoexit", url]
        try:
            self.play_process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.btn_play.config(text="⏹ STOP STREAM")
        except Exception as e: self.log(f"PLAYBACK ERROR: {e}")

    def stop_playback(self):
        if self.play_process:
            self.log("PLAYBACK: Stopping...")
            self.play_process.terminate()
            self.play_process = None
        self.btn_play.config(text="▶ PLAY STREAM")

    # -------------------- Control --------------------
    def start_process(self):
        ffmpeg = self.ffmpeg_path.get().strip()
        if not ffmpeg or not os.path.exists(ffmpeg): return messagebox.showerror("Error", "FFmpeg not found!")
        out_dir = self.output_path.get().strip()
        if not out_dir or not os.path.isdir(out_dir): return messagebox.showerror("Error", "Output folder does not exist.")
        
        # Debug parsing
        raw_total = self.total_time_str.get()
        raw_chunk = self.chunk_time_str.get()
        total_sec = parse_time_string(raw_total)
        chunk_sec = parse_time_string(raw_chunk)
        
        self.log(f"DEBUG: Parsed Total='{raw_total}'->{total_sec}s, Chunk='{raw_chunk}'->{chunk_sec}s")

        if total_sec <= 0 or chunk_sec <= 0: return messagebox.showerror("Error", "Total time and chunk length must be > 0.")
        
        self.persist_defaults_from_ui() 
        stream_url = self.url.get().strip()
        self._chunks_ok = 0
        self._chunks_fail = 0
        self._user_stopped = False
        now = datetime.datetime.now()
        self.session_id = now.strftime("%Y%m%d_%H%M%S")
        preset_name = self.selected_preset.get()
        short_code = station_short_code(station_name=preset_name, preset_name=preset_name)

        self.manifest = SessionManifest(
            out_dir=out_dir, session_id=self.session_id, app_name=APP_NAME, app_version=APP_VERSION,
            station_url=stream_url, preset_name=preset_name, short_code=short_code,
            chunk_seconds=chunk_sec, tape_mode=False, output_format=self.output_format.get(),
        )
        self.log(f"Session manifest: STROAD_Rec_{self.session_id}.session.json")
        self.stop_requested = False
        self.is_running = True
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.pb_chunk["value"] = 0
        self.pb_total["value"] = 0
        self.status_text.set("Starting pipeline…")
        self.process_thread = threading.Thread(target=self.worker_process, daemon=True)
        self.capture_thread = threading.Thread(target=self.worker_capture, daemon=True)
        self.process_thread.start()
        self.capture_thread.start()

    def stop_process(self):
        if not self.is_running: return
        self._user_stopped = True
        self.log("STOP requested...")
        if self.manifest: self.manifest.event("stop_requested")
        self.stop_requested = True
        if self.current_process:
            try: self.current_process.terminate()
            except Exception: pass

    def reset_buttons(self):
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.status_text.set("Idle.")
        self.chunk_progress_text.set("Chunk: -/-")
        self.time_progress_text.set("Time: 00:00 / 00:00")
        self.pb_chunk["value"] = 0
        self.pb_total["value"] = 0

    # -------------------- Capture Workers --------------------
    def _stderr_tail(self, lines: List[str], max_lines: int = 12) -> List[str]:
        if not lines: return []
        return lines[-max_lines:]

    def _looks_like_transient_http(self, stderr_text: str) -> bool:
        t = (stderr_text or "").lower()
        return any(n in t for n in ["http error 503", "server returned 5xx", "error opening input", "service unavailable", "connection refused", "connection reset", "timed out", "temporary failure"])

    def _run_capture_ffmpeg_with_progress(self, ffmpeg: str, stream_url: str, dur: int, temp_file: str) -> Tuple[int, str]:
        cmd = [ffmpeg, "-y", "-re", "-i", stream_url, "-t", str(dur), "-map_metadata", "0", "-vn", "-c", "copy", "-f", "matroska", "-nostats", temp_file]
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.current_process = p
        stderr_lines = deque(maxlen=400)
        stop_reader = threading.Event()
        def _reader():
            try:
                if p.stderr:
                    for line in p.stderr:
                        if stop_reader.is_set(): break
                        stderr_lines.append(line.rstrip("\n"))
            except Exception: pass
        t_reader = threading.Thread(target=_reader, daemon=True)
        t_reader.start()
        start_wall = time.time()
        last_ui_sec = -1
        try:
            while True:
                if self.stop_requested:
                    try: p.terminate()
                    except Exception: pass
                    break
                rc = p.poll()
                elapsed = int(time.time() - start_wall)
                if elapsed != last_ui_sec:
                    last_ui_sec = elapsed
                    self.root.after(0, lambda e=elapsed: [self.pb_chunk.configure(value=min(dur, max(0, e))), self.time_progress_text.set(f"Time: {fmt_mmss(min(dur, max(0, e)))} / {fmt_mmss(dur)}")])
                if rc is not None or elapsed >= dur: break
                time.sleep(0.2)
            try: p.wait(timeout=2.0)
            except Exception: 
                try: p.terminate(); p.wait(timeout=2.0)
                except Exception: pass
        finally:
            stop_reader.set()
            try: p.stderr.close()
            except Exception: pass
            t_reader.join(timeout=0.5)
            self.current_process = None
        return (p.returncode if p.returncode is not None else -1, "\n".join(list(stderr_lines)))

    def worker_capture(self):
        try:
            total_sec = parse_time_string(self.total_time_str.get())
            chunk_sec = parse_time_string(self.chunk_time_str.get())
            ffmpeg = self.ffmpeg_path.get().strip()
            out_dir = self.output_path.get().strip()
            prefix = (self.filename_prefix.get().strip() or "STROAD_Rec")
            stream_url = self.url.get().strip()
            num_chunks = (total_sec + chunk_sec - 1) // chunk_sec
            self.root.after(0, lambda: self.pb_total.configure(maximum=max(1, num_chunks)))
            self.log(f"CAPTURE: {num_chunks} chunks planned.")
            self.root.after(0, lambda: self.status_text.set("Capturing…"))
            if self.manifest: self.manifest.event("capture_start", planned_chunks=num_chunks)

            for i in range(1, num_chunks + 1):
                if self.stop_requested: break
                dur = chunk_sec
                if i == num_chunks:
                    rem = total_sec % chunk_sec
                    if rem > 0: dur = rem
                start_dt = datetime.datetime.now()
                start_iso = start_dt.astimezone().isoformat(timespec="seconds")
                title_range = fmt_title_range(start_dt, dur)
                tags = ffprobe_tags(self.ffprobe_path.get().strip(), stream_url)
                station = station_name_from_tags(tags, self.selected_preset.get())
                temp_file = os.path.join(out_dir, "stroad_raw_%s_%s.mka" % (os.getpid(), uuid.uuid4().hex[:8]))
                ts = start_dt.strftime("%Y%m%d_%H%M%S")
                out_ext = ".mp3" if "MP3" in self.output_format.get() else ".m4a"
                final_file = os.path.join(out_dir, "%s_%s_%03d%s" % (prefix, ts, i, out_ext))
                self.root.after(0, lambda: [self.chunk_progress_text.set("Chunk: %d/%d" % (i, num_chunks)), self.time_progress_text.set("Time: 00:00 / %s" % fmt_mmss(dur)), self.pb_chunk.configure(maximum=max(1, dur), value=0), self.pb_total.configure(value=i-1)])
                self.log("CAPTURE %d/%d: %ds | album='%s' | title='%s'" % (i, num_chunks, dur, station, title_range))
                
                # Retry loop
                max_retries = 3
                ok_temp = False
                rc = -1
                err = ""
                for attempt in range(max_retries + 1):
                    if self.stop_requested: break
                    if attempt > 0:
                        wait = [1, 2, 4][min(attempt-1, 2)]
                        self.log(f"CAPTURE retry {attempt}/{max_retries} after {wait}s...")
                        if self.manifest: self.manifest.event("retry_connect", chunk=i, attempt=attempt)
                        time.sleep(wait)
                    if os.path.exists(temp_file): os.remove(temp_file)
                    rc, err = self._run_capture_ffmpeg_with_progress(ffmpeg, stream_url, dur, temp_file)
                    if os.path.exists(temp_file) and os.path.getsize(temp_file) >= 20000:
                        ok_temp = True; break
                    if not self._looks_like_transient_http(err): break
                
                if self.stop_requested: 
                    if os.path.exists(temp_file): os.remove(temp_file)
                    break
                if not ok_temp:
                    self.log("CAPTURE FAILED. Stderr tail:"); 
                    for l in self._stderr_tail(err.splitlines()): self.log("  "+l)
                    self._chunks_fail += 1
                    if self.manifest: self.manifest.error(f"Capture failed chunk {i}", exit_code=rc)
                    if os.path.exists(temp_file): os.remove(temp_file)
                    continue

                end_dt = datetime.datetime.now()
                job = {"i": i, "num_chunks": num_chunks, "dur": dur, "actual_seconds": float(dur), "start_iso": start_iso, "end_iso": end_dt.astimezone().isoformat(timespec="seconds"), "temp_file": temp_file, "final_file": final_file, "album": station, "artist": prefix, "title": title_range, "year": start_dt.year}
                self.job_q.put(job)
                self.log(f"ENQUEUED: {os.path.basename(final_file)}")

            self.log("CAPTURE: finished (or stopped).")
            if self.manifest: self.manifest.event("capture_end")
        except Exception as e:
            self.log(f"CAPTURE CRITICAL ERROR: {e}")
            if self.manifest: self.manifest.error(f"Capture critical: {e}")
        finally: self.job_q.put(None)

    def worker_process(self):
        try:
            ffmpeg = self.ffmpeg_path.get().strip()
            fade_sec = safe_int(self.fade_duration.get(), default=0)
            self.log("PROCESSOR: ready.")
            if self.manifest: self.manifest.event("processor_ready")
            while True:
                job = self.job_q.get()
                if job is None: break
                i = job["i"]
                self.root.after(0, lambda: self.status_text.set("Processing…"))
                self.log(f"PROCESS {i}: tagging -> {os.path.basename(job['final_file'])}")
                fade_filter = "anull"
                if fade_sec > 0: fade_filter = f"afade=t=in:ss=0:d={fade_sec},areverse,afade=t=in:ss=0:d={fade_sec},areverse"
                out_ext = os.path.splitext(job['final_file'])[1].lower()
                acodec = ["-c:a", "libmp3lame", "-q:a", "4"] if out_ext == ".mp3" else ["-c:a", "aac", "-b:a", "192k"]
                cmd = [ffmpeg, "-y", "-i", job['temp_file'], "-af", fade_filter, "-metadata", f"album={job['album']}", "-metadata", f"artist={job['artist']}", "-metadata", f"title={job['title']}", "-metadata", f"date={job['year']}"] + acodec + [job['final_file']]
                res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try: os.remove(job['temp_file'])
                except: pass
                if res.returncode == 0 and os.path.exists(job['final_file']):
                    self._chunks_ok += 1
                    self.log(f"SAVED: {os.path.basename(job['final_file'])}")
                    if self.manifest: self.manifest.add_chunk(index=i, start_local=job['start_iso'], end_local=job['end_iso'], planned_seconds=job['dur'], actual_seconds=job['actual_seconds'], output_file=os.path.basename(job['final_file']), bytes_written=os.path.getsize(job['final_file']), ffmpeg_exit_code=0)
                else: self._chunks_fail += 1
                self.root.after(0, lambda v=i: self.pb_total.configure(value=v))
            self.log("PROCESSOR: finished.")
        except Exception as e: self.log(f"PROCESS ERROR: {e}")
        finally:
            self.is_running = False; self.current_process = None
            if self.manifest: self.manifest.finalize("completed" if self._chunks_ok > 0 else "aborted")
            self.root.after(0, self.reset_buttons)