import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import subprocess
import threading
import os
import time
import datetime
import queue
import uuid

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
        self.root.geometry("680x760")

        # --- PRESETS ---
        self.presets = {
            "Jazz24 (128k MP3)": "https://knkx-live-a.edge.audiocdn.com/6285_128k",
            "Jazz24 (256k AAC)": "https://knkx-live-a.edge.audiocdn.com/6285_256k",
            "BBC Radio 1 (HLS)": "http://as-hls-ww-live.akamaized.net/pool_904/live/ww/bbc_radio_one/bbc_radio_one.isml/bbc_radio_one-audio=96000.norewind.m3u8",
            "SomaFM Groove Salad": "http://ice1.somafm.com/groovesalad-128-mp3",
            "Custom URL": ""
        }

        # Load persisted settings
        self.cfg = load_settings()

        # --- Variables ---
        self.ffmpeg_path = tk.StringVar(value=self.cfg.get("ffmpeg_path") or self.find_bin("ffmpeg"))
        self.ffprobe_path = tk.StringVar(value=self.cfg.get("ffprobe_path") or self.find_bin("ffprobe"))

        self.selected_preset = tk.StringVar(value=self.cfg.get("selected_preset") or "Jazz24 (128k MP3)")
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
        self._session_aborted = False

        # Thread-safe UI logging
        self.log_q = queue.Queue()

        # Pipeline queue: capture -> process
        self.job_q = queue.Queue()
        self.capture_thread = None
        self.process_thread = None

        # UI vars
        self.status_text = tk.StringVar(value="Idle.")
        self.chunk_progress_text = tk.StringVar(value="Chunk: -/-")
        self.time_progress_text = tk.StringVar(value="Time: 00:00 / 00:00")

        # Apply theme (affects ttk)
        self.palette = apply_theme(self.root, self.theme_name.get())

        self.build_ui()
        self.root.after(80, self._pump_log_queue)

    # -------------------- Theme & settings --------------------
    def persist_defaults_from_ui(self):
        values = {
            "theme": self.theme_name.get(),

            "ffmpeg_path": self.ffmpeg_path.get().strip(),
            "ffprobe_path": self.ffprobe_path.get().strip(),

            "selected_preset": self.selected_preset.get(),
            "custom_url": self.url.get().strip() if self.selected_preset.get() == "Custom URL" else self.cfg.get("custom_url",""),

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
        win.geometry("520x420")
        win.transient(self.root)
        win.grab_set()

        frm = ttk.Frame(win, padding=10)
        frm.pack(fill="both", expand=True)

        # Theme row
        ttk.Label(frm, text="Theme:").grid(row=0, column=0, sticky="w", pady=6)
        theme_cb = ttk.Combobox(frm, textvariable=self.theme_name, values=list(THEMES.keys()), state="readonly", width=18)
        theme_cb.grid(row=0, column=1, sticky="w", pady=6)

        def _apply_theme_now():
            self.palette = apply_theme(self.root, self.theme_name.get())
            # Also update log colors (non-ttk widget)
            self._apply_log_colors()
        ttk.Button(frm, text="Apply Theme", command=_apply_theme_now).grid(row=0, column=2, padx=8, pady=6)

        # Defaults rows (we just show current UI values; Save Defaults persists them)
        sep = ttk.Separator(frm, orient="horizontal")
        sep.grid(row=1, column=0, columnspan=3, sticky="ew", pady=10)

        ttk.Label(frm, text="Defaults saved in ~/.stroad2.json").grid(row=2, column=0, columnspan=3, sticky="w", pady=(0,10))

        # ffmpeg/ffprobe
        ttk.Label(frm, text="FFmpeg:").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.ffmpeg_path).grid(row=3, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="...", width=3, command=lambda: self._pick_file(self.ffmpeg_path)).grid(row=3, column=2, padx=6)

        ttk.Label(frm, text="FFprobe:").grid(row=4, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.ffprobe_path).grid(row=4, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="...", width=3, command=lambda: self._pick_file(self.ffprobe_path)).grid(row=4, column=2, padx=6)

        # output dir
        ttk.Label(frm, text="Default output folder:").grid(row=5, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.output_path).grid(row=5, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="...", width=3, command=lambda: self._pick_dir(self.output_path)).grid(row=5, column=2, padx=6)

        # times / format
        ttk.Label(frm, text="Total time:").grid(row=6, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.total_time_str).grid(row=6, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Chunk length:").grid(row=7, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.chunk_time_str).grid(row=7, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Output format:").grid(row=8, column=0, sticky="w", pady=4)
        ttk.Combobox(frm, textvariable=self.output_format, values=self.output_format_options, state="readonly").grid(row=8, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Filename prefix:").grid(row=9, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.filename_prefix).grid(row=9, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Fade (s):").grid(row=10, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=self.fade_duration, width=8).grid(row=10, column=1, sticky="w", pady=4)

        # Buttons
        btns = ttk.Frame(frm)
        btns.grid(row=11, column=0, columnspan=3, sticky="e", pady=16)

        def _save_defaults():
            self.persist_defaults_from_ui()
            messagebox.showinfo("Saved", "Defaults saved to ~/.stroad2.json")
        ttk.Button(btns, text="Save Defaults", command=_save_defaults).pack(side="right", padx=6)
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")

        frm.columnconfigure(1, weight=1)

    def _pick_file(self, var):
        picked = filedialog.askopenfilename()
        if picked:
            var.set(picked)

    def _pick_dir(self, var):
        picked = filedialog.askdirectory()
        if picked:
            var.set(picked)

    # -------------------- Basic helpers --------------------
    def find_bin(self, name: str) -> str:
        import shutil
        return shutil.which(name) or ""

    # -------------------- Logging (thread-safe) --------------------
    def log(self, msg: str):
        self.log_q.put(log_line(msg))

    def _pump_log_queue(self):
        try:
            while True:
                line = self.log_q.get_nowait()
                self.log_area.insert("end", f"> {line}\n")
                self.log_area.see("end")
        except queue.Empty:
            pass
        self.root.after(80, self._pump_log_queue)

    # -------------------- UI --------------------
    def on_preset_change(self, event=None):
        choice = self.selected_preset.get()
        if choice == "Custom URL":
            # keep current URL as custom
            return
        self.url.set(self.presets.get(choice, ""))

    def build_ui(self):
        f_conf = ttk.LabelFrame(self.root, text="Configuration")
        f_conf.pack(fill="x", padx=10, pady=6)

        self._add_row(f_conf, 0, "FFmpeg:", self.ffmpeg_path, browse=True)
        self._add_row(f_conf, 1, "FFprobe:", self.ffprobe_path, browse=True)

        ttk.Label(f_conf, text="Stream Source:").grid(row=2, column=0, sticky="w", padx=5)
        combo = ttk.Combobox(f_conf, textvariable=self.selected_preset, values=list(self.presets.keys()), state="readonly")
        combo.grid(row=2, column=1, sticky="ew", padx=5, pady=5)
        combo.bind("<<ComboboxSelected>>", self.on_preset_change)

        ttk.Label(f_conf, text="URL:").grid(row=3, column=0, sticky="w", padx=5)
        ttk.Entry(f_conf, textvariable=self.url).grid(row=3, column=1, sticky="ew", padx=5, pady=5)

        self._add_row(f_conf, 4, "Save To:", self.output_path, browse_dir=True)

        ttk.Label(f_conf, text="Output:").grid(row=5, column=0, sticky="w", padx=5)
        ttk.Combobox(
            f_conf,
            textvariable=self.output_format,
            values=self.output_format_options,
            state="readonly"
        ).grid(row=5, column=1, sticky="ew", padx=5, pady=5)

        # preferences button
        ttk.Button(f_conf, text="Preferences…", command=self.open_preferences).grid(row=6, column=1, sticky="e", padx=5, pady=(8,5))

        f_conf.columnconfigure(1, weight=1)

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

        f_act = ttk.Frame(self.root)
        f_act.pack(fill="x", padx=10, pady=8)

        self.btn_start = ttk.Button(f_act, text="START RECORDING", command=self.start_process)
        self.btn_start.pack(side="left", fill="x", expand=True, padx=5)

        self.btn_stop = ttk.Button(f_act, text="STOP & FADE", command=self.stop_process, state="disabled")
        self.btn_stop.pack(side="right", padx=5)

        f_prog = ttk.LabelFrame(self.root, text="Progress")
        f_prog.pack(fill="x", padx=10, pady=6)

        ttk.Label(f_prog, textvariable=self.status_text).pack(anchor="w", padx=8, pady=2)
        ttk.Label(f_prog, textvariable=self.chunk_progress_text).pack(anchor="w", padx=8, pady=2)
        ttk.Label(f_prog, textvariable=self.time_progress_text).pack(anchor="w", padx=8, pady=2)

        self.pb_chunk = ttk.Progressbar(f_prog, orient="horizontal", mode="determinate", maximum=100)
        self.pb_chunk.pack(fill="x", padx=8, pady=6)

        self.pb_total = ttk.Progressbar(f_prog, orient="horizontal", mode="determinate", maximum=100)
        self.pb_total.pack(fill="x", padx=8, pady=(0, 8))

        self.log_area = scrolledtext.ScrolledText(
            self.root,
            height=16,
            bg=self.palette["bg"],
            fg=self.palette["accent"],
            insertbackground=self.palette["text"],
            font=("Menlo", 11)
        )
        self.log_area.pack(fill="both", expand=True, padx=10, pady=6)

        self._apply_log_colors()

    def _apply_log_colors(self):
        # For System, keep it readable
        if self.theme_name.get() == "System":
            self.log_area.configure(bg="white", fg="black", insertbackground="black")
        else:
            self.log_area.configure(bg=self.palette["bg"], fg=self.palette["accent"], insertbackground=self.palette["text"])

    def _add_row(self, parent, row, label, var, browse=False, browse_dir=False):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=5, pady=5)
        ttk.Entry(parent, textvariable=var).grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        if browse or browse_dir:
            def cmd():
                if browse_dir:
                    picked = filedialog.askdirectory()
                else:
                    picked = filedialog.askopenfilename()
                if picked:
                    var.set(picked)
            ttk.Button(parent, text="...", width=3, command=cmd).grid(row=row, column=2, padx=5)

    # -------------------- Control --------------------
    def start_process(self):
        ffmpeg = self.ffmpeg_path.get().strip()
        if not ffmpeg or not os.path.exists(ffmpeg):
            messagebox.showerror("Error", "FFmpeg not found!")
            return

        out_dir = self.output_path.get().strip()
        if not out_dir or not os.path.isdir(out_dir):
            messagebox.showerror("Error", "Output folder does not exist.")
            return

        total_sec = parse_time_string(self.total_time_str.get())
        chunk_sec = parse_time_string(self.chunk_time_str.get())
        if total_sec <= 0 or chunk_sec <= 0:
            messagebox.showerror("Error", "Total time and chunk length must be > 0.")
            return

        stream_url = self.url.get().strip()
        if not stream_url:
            messagebox.showerror("Error", "Stream URL is empty.")
            return

        # Start a new session manifest (safe: one file + boundary updates)
        now = datetime.datetime.now()
        self.session_id = now.strftime("%Y%m%d_%H%M%S")
        preset_name = self.selected_preset.get()
        # We may refine station name after ffprobe, but keep preset-based code stable:
        short_code = station_short_code(station_name=preset_name, preset_name=preset_name)

        self.manifest = SessionManifest(
            out_dir=out_dir,
            session_id=self.session_id,
            app_name=APP_NAME,
            app_version=APP_VERSION,
            station_url=stream_url,
            preset_name=preset_name,
            short_code=short_code,
            chunk_seconds=chunk_sec,
            tape_mode=False,
            output_format=self.output_format.get(),
        )

        self.log(f"Session manifest: STROAD_Rec_{self.session_id}.session.json")

        # Persist defaults (optional: treat current state as last-used)
        self.persist_defaults_from_ui()

        self.stop_requested = False
        self._session_aborted = False
        self.is_running = True

        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")

        self.pb_chunk["value"] = 0
        self.pb_total["value"] = 0
        self.status_text.set("Starting pipeline…")

        # Start processor first, then capture
        self.process_thread = threading.Thread(target=self.worker_process, daemon=True)
        self.capture_thread = threading.Thread(target=self.worker_capture, daemon=True)
        self.process_thread.start()
        self.capture_thread.start()

    def stop_process(self):
        if not self.is_running:
            return
        self.log("STOP requested: stopping capture (processor will finish queued chunks).")
        if self.manifest:
            self.manifest.event("stop_requested")
        self.stop_requested = True
        if self.current_process:
            try:
                self.current_process.terminate()
            except Exception:
                pass

    def reset_buttons(self):
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.status_text.set("Idle.")
        self.chunk_progress_text.set("Chunk: -/-")
        self.time_progress_text.set("Time: 00:00 / 00:00")
        self.pb_chunk["value"] = 0
        self.pb_total["value"] = 0

    # -------------------- Thread A: CAPTURE ONLY --------------------
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
            if self.manifest:
                self.manifest.event("capture_start", planned_chunks=num_chunks)

            for i in range(1, num_chunks + 1):
                if self.stop_requested:
                    break

                dur = chunk_sec
                if i == num_chunks:
                    rem = total_sec % chunk_sec
                    if rem > 0:
                        dur = rem

                start_dt = datetime.datetime.now()
                start_iso = start_dt.astimezone().isoformat(timespec="seconds")
                title_range = fmt_title_range(start_dt, dur)

                tags = ffprobe_tags(self.ffprobe_path.get().strip(), stream_url)
                station = station_name_from_tags(tags, self.selected_preset.get())

                temp_file = os.path.join(out_dir, f"stroad_raw_{os.getpid()}_{uuid.uuid4().hex[:8]}.mka")

                ts = start_dt.strftime("%Y%m%d_%H%M%S")
                out_choice = self.output_format.get()
                out_ext = ".mp3" if "MP3" in out_choice else ".m4a"
                final_file = os.path.join(out_dir, f"{prefix}_{ts}_{i:03d}{out_ext}")

                def _ui_start():
                    self.chunk_progress_text.set(f"Chunk: {i}/{num_chunks}")
                    self.time_progress_text.set(f"Time: 00:00 / {fmt_mmss(dur)}")
                    self.pb_chunk.configure(maximum=max(1, dur))
                    self.pb_chunk["value"] = 0
                    self.pb_total["value"] = i - 1
                self.root.after(0, _ui_start)

                self.log(f"CAPTURE {i}/{num_chunks}: {dur}s | album='{station}' | title='{title_range}'")

                cmd = [
                    ffmpeg, "-y",
                    "-re",
                    "-i", stream_url,
                    "-t", str(dur),
                    "-map_metadata", "0",
                    "-vn",
                    "-c", "copy",
                    "-f", "matroska",
                    "-nostats",
                    temp_file
                ]

                self.current_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                start_wall = time.time()
                last_ui_sec = -1

                while True:
                    if self.stop_requested:
                        break

                    elapsed = int(time.time() - start_wall)
                    if elapsed != last_ui_sec:
                        last_ui_sec = elapsed
                        def _ui_tick(e=elapsed):
                            e2 = min(dur, max(0, e))
                            self.pb_chunk["value"] = e2
                            self.time_progress_text.set(f"Time: {fmt_mmss(e2)} / {fmt_mmss(dur)}")
                        self.root.after(0, _ui_tick)

                    if self.current_process.poll() is not None:
                        break
                    if elapsed >= dur:
                        break

                    time.sleep(0.1)

                err = ""
                try:
                    _, err = self.current_process.communicate(timeout=1.0)
                except Exception:
                    try:
                        self.current_process.terminate()
                    except Exception:
                        pass
                exit_code = self.current_process.returncode if self.current_process else -1
                self.current_process = None

                elapsed_wall = time.time() - start_wall
                if (not self.stop_requested) and (elapsed_wall < dur):
                    time.sleep(dur - elapsed_wall)

                end_dt = start_dt + datetime.timedelta(seconds=float(min(dur, elapsed_wall)))
                end_iso = end_dt.astimezone().isoformat(timespec="seconds")

                def _ui_full():
                    self.pb_chunk["value"] = dur
                    self.time_progress_text.set(f"Time: {fmt_mmss(dur)} / {fmt_mmss(dur)}")

                if (not os.path.exists(temp_file)) or (os.path.getsize(temp_file) < 20_000):
                    tail = (err or "").strip().splitlines()[-10:]
                    if tail:
                        self.log("CAPTURE FAILED: ffmpeg stderr tail:")
                        for line in tail:
                            self.log("  " + line[:300])
                    else:
                        self.log("CAPTURE FAILED: produced no/too-small file (no stderr captured).")
                    if self.manifest:
                        self.manifest.error(f"Capture failed for chunk {i} (no/too-small temp file).", exit_code=exit_code)

                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
                    self._session_aborted = True
                    continue

                self.root.after(0, _ui_full)

                job = {
                    "i": i,
                    "num_chunks": num_chunks,
                    "dur": dur,
                    "actual_seconds": float(min(dur, elapsed_wall)),
                    "start_iso": start_iso,
                    "end_iso": end_iso,
                    "temp_file": temp_file,
                    "final_file": final_file,
                    "album": station,
                    "artist": prefix,
                    "title": title_range,
                    "year": start_dt.year
                }
                self.job_q.put(job)
                self.log(f"ENQUEUED: {os.path.basename(final_file)}")

            self.log("CAPTURE: finished (or stopped).")
            if self.manifest:
                self.manifest.event("capture_end")

        except Exception as e:
            self.log(f"CAPTURE CRITICAL ERROR: {e}")
            if self.manifest:
                self.manifest.error(f"Capture critical error: {e}")

            self._session_aborted = True

        finally:
            self.job_q.put(None)

    # -------------------- Thread B: PROCESS ONLY --------------------
    def worker_process(self):
        try:
            ffmpeg = self.ffmpeg_path.get().strip()
            fade_sec = safe_int(self.fade_duration.get(), default=0)

            self.log("PROCESSOR: ready.")
            if self.manifest:
                self.manifest.event("processor_ready")

            while True:
                job = self.job_q.get()
                if job is None:
                    break

                i = job["i"]
                num_chunks = job["num_chunks"]
                temp_file = job["temp_file"]
                final_file = job["final_file"]

                album = job["album"]
                artist = job["artist"]
                title = job["title"]
                year = job["year"]

                start_iso = job.get("start_iso")
                end_iso = job.get("end_iso")
                planned = int(job.get("dur") or 0)
                actual = float(job.get("actual_seconds") or planned)

                self.root.after(0, lambda: self.status_text.set("Processing…"))
                self.log(f"PROCESS {i}/{num_chunks}: tagging album/artist/title -> {os.path.basename(final_file)}")

                fade_filter = "anull"
                if fade_sec > 0:
                    fade_filter = f"afade=t=in:ss=0:d={fade_sec},areverse,afade=t=in:ss=0:d={fade_sec},areverse"

                out_ext = os.path.splitext(final_file)[1].lower()
                if out_ext == ".mp3":
                    audio_codec = ["-c:a", "libmp3lame", "-q:a", "4"]
                else:
                    audio_codec = ["-c:a", "aac", "-b:a", "192k"]

                cmd = [
                    ffmpeg, "-y",
                    "-i", temp_file,
                    "-af", fade_filter,
                    "-metadata", f"album={album}",
                    "-metadata", f"artist={artist}",
                    "-metadata", f"title={title}",
                    "-metadata", f"date={year}",
                ] + audio_codec + [final_file]

                res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                exit_code = int(res.returncode)

                try:
                    os.remove(temp_file)
                except Exception:
                    pass

                bytes_written = 0
                try:
                    bytes_written = os.path.getsize(final_file)
                except Exception:
                    bytes_written = 0

                self.log(f"SAVED: {os.path.basename(final_file)} ({bytes_written} bytes)")
                self.root.after(0, lambda v=i: self.pb_total.configure(value=v))

                if self.manifest:
                    self.manifest.add_chunk(
                        index=i,
                        start_local=start_iso,
                        end_local=end_iso,
                        planned_seconds=planned,
                        actual_seconds=actual,
                        output_file=os.path.basename(final_file),
                        bytes_written=bytes_written,
                        ffmpeg_exit_code=exit_code,
                    )
                    self.manifest.event("chunk_complete", index=i)

                if exit_code != 0:
                    self._session_aborted = True
                    if self.manifest:
                        self.manifest.error(f"Processing ffmpeg failed for chunk {i}.", exit_code=exit_code)

            self.log("PROCESSOR: finished.")

        except Exception as e:
            self.log(f"PROCESS CRITICAL ERROR: {e}")
            if self.manifest:
                self.manifest.error(f"Processor critical error: {e}")
            self._session_aborted = True

        finally:
            self.is_running = False
            self.current_process = None
            status = "aborted" if (self.stop_requested or self._session_aborted) else "completed"
            if self.manifest:
                self.manifest.finalize(status)
            self.root.after(0, self.reset_buttons)
