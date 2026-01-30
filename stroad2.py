import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import subprocess
import threading
import os
import re
import shutil
import time
import datetime
import queue
import uuid
import json

class StroadGold:
    def __init__(self, root):
        self.root = root
        self.root.title("STROAD 2.0 Gold")
        self.root.geometry("680x760")

        # ---- Unified gray theme colors ----
        self.C_BG = "#2b2b2b"
        self.C_PANEL = "#333333"
        self.C_FIELD = "#3a3a3a"
        self.C_TEXT = "#e6e6e6"
        self.C_ACCENT = "#00ff00"
        self.C_BORDER = "#444444"

        self.apply_theme()

        # --- PRESETS ---
        self.presets = {
            "Jazz24 (128k MP3)": "https://knkx-live-a.edge.audiocdn.com/6285_128k",
            "Jazz24 (256k AAC)": "https://knkx-live-a.edge.audiocdn.com/6285_256k",
            "BBC Radio 1 (HLS)": "http://as-hls-ww-live.akamaized.net/pool_904/live/ww/bbc_radio_one/bbc_radio_one.isml/bbc_radio_one-audio=96000.norewind.m3u8",
            "SomaFM Groove Salad": "http://ice1.somafm.com/groovesalad-128-mp3",
            "Custom URL": ""
        }

        # --- Variables ---
        self.ffmpeg_path = tk.StringVar(value=self.find_bin("ffmpeg"))
        self.ffprobe_path = tk.StringVar(value=self.find_bin("ffprobe"))

        self.selected_preset = tk.StringVar(value="Jazz24 (128k MP3)")
        self.url = tk.StringVar(value=self.presets["Jazz24 (128k MP3)"])
        self.total_time_str = tk.StringVar(value="1h 00m")
        self.chunk_time_str = tk.StringVar(value="15m")
        self.fade_duration = tk.StringVar(value="3")
        self.filename_prefix = tk.StringVar(value="STROAD_Rec")
        self.output_path = tk.StringVar(value=os.path.expanduser("~/Downloads"))

        self.output_format = tk.StringVar(value="MP3 (encoded)")
        self.output_format_options = ["MP3 (encoded)", "M4A (AAC encoded)"]

        # --- Runtime state ---
        self.is_running = False
        self.stop_requested = False
        self.current_process = None

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

        self.build_ui()
        self.root.after(80, self._pump_log_queue)

    # -------------------- Theme --------------------
    def apply_theme(self):
        self.root.configure(bg=self.C_BG)

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure(".", background=self.C_BG, foreground=self.C_TEXT)
        style.configure("TFrame", background=self.C_BG)
        style.configure("TLabelframe", background=self.C_BG, foreground=self.C_TEXT)
        style.configure("TLabelframe.Label", background=self.C_BG, foreground=self.C_TEXT)
        style.configure("TLabel", background=self.C_BG, foreground=self.C_TEXT)

        style.configure("TButton", background=self.C_PANEL, foreground=self.C_TEXT, bordercolor=self.C_BORDER)
        style.map(
            "TButton",
            background=[("active", "#3d3d3d"), ("disabled", "#2a2a2a")],
            foreground=[("disabled", "#888888")]
        )

        style.configure(
            "TEntry",
            fieldbackground=self.C_FIELD,
            foreground=self.C_TEXT,
            background=self.C_PANEL,
            bordercolor=self.C_BORDER,
            insertcolor=self.C_TEXT
        )
        style.configure(
            "TCombobox",
            fieldbackground=self.C_FIELD,
            foreground=self.C_TEXT,
            background=self.C_PANEL,
            bordercolor=self.C_BORDER,
            arrowcolor=self.C_TEXT
        )
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", self.C_FIELD)],
            background=[("readonly", self.C_PANEL)],
            foreground=[("readonly", self.C_TEXT)]
        )

        style.configure("Horizontal.TProgressbar", background="#666666", troughcolor=self.C_FIELD)

    # -------------------- Basic helpers --------------------
    def find_bin(self, name):
        return shutil.which(name) or ""

    def parse_time_string(self, time_str):
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

    def safe_int(self, s, default=0):
        try:
            return int(str(s).strip())
        except:
            return default

    def fmt_mmss(self, seconds):
        seconds = max(0, int(seconds))
        m, s = divmod(seconds, 60)
        return f"{m:02d}:{s:02d}"

    def fmt_title_range(self, start_dt, dur_seconds):
        end_dt = start_dt + datetime.timedelta(seconds=int(dur_seconds))
        if dur_seconds < 600:
            return f"{start_dt:%Y-%m-%d %H:%M:%S}–{end_dt:%H:%M:%S}"
        return f"{start_dt:%Y-%m-%d %H:%M}–{end_dt:%H:%M}"

    # -------------------- Logging (thread-safe) --------------------
    def log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}")

    def _pump_log_queue(self):
        try:
            while True:
                line = self.log_q.get_nowait()
                self.log_area.insert("end", f"> {line}\n")
                self.log_area.see("end")
        except queue.Empty:
            pass
        self.root.after(80, self._pump_log_queue)

    # -------------------- ffprobe metadata --------------------
    def ffprobe_tags(self, stream_url, timeout=6):
        ffprobe = self.ffprobe_path.get().strip()
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

    def station_name_from_tags(self, tags):
        name = (tags.get("icy-name") or tags.get("icy_name") or "").strip()
        if name:
            return name
        preset = (self.selected_preset.get() or "").strip()
        if preset and preset.lower() != "custom url":
            return preset
        return "STROAD Radio"

    # -------------------- UI --------------------
    def on_preset_change(self, event=None):
        choice = self.selected_preset.get()
        self.url.set("" if choice == "Custom URL" else self.presets[choice])

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
            bg=self.C_BG,
            fg=self.C_ACCENT,
            insertbackground=self.C_TEXT,
            font=("Menlo", 11)
        )
        self.log_area.pack(fill="both", expand=True, padx=10, pady=6)

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

        total_sec = self.parse_time_string(self.total_time_str.get())
        chunk_sec = self.parse_time_string(self.chunk_time_str.get())
        if total_sec <= 0 or chunk_sec <= 0:
            messagebox.showerror("Error", "Total time and chunk length must be > 0.")
            return

        stream_url = self.url.get().strip()
        if not stream_url:
            messagebox.showerror("Error", "Stream URL is empty.")
            return

        self.stop_requested = False
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
            total_sec = self.parse_time_string(self.total_time_str.get())
            chunk_sec = self.parse_time_string(self.chunk_time_str.get())
            ffmpeg = self.ffmpeg_path.get().strip()
            out_dir = self.output_path.get().strip()
            prefix = (self.filename_prefix.get().strip() or "STROAD_Rec")
            stream_url = self.url.get().strip()

            num_chunks = (total_sec + chunk_sec - 1) // chunk_sec
            self.root.after(0, lambda: self.pb_total.configure(maximum=max(1, num_chunks)))

            self.log(f"CAPTURE: {num_chunks} chunks planned.")
            self.root.after(0, lambda: self.status_text.set("Capturing…"))

            for i in range(1, num_chunks + 1):
                if self.stop_requested:
                    break

                dur = chunk_sec
                if i == num_chunks:
                    rem = total_sec % chunk_sec
                    if rem > 0:
                        dur = rem

                start_dt = datetime.datetime.now()
                title_range = self.fmt_title_range(start_dt, dur)

                tags = self.ffprobe_tags(stream_url)
                station = self.station_name_from_tags(tags)

                temp_file = os.path.join(out_dir, f"stroad_raw_{os.getpid()}_{uuid.uuid4().hex[:8]}.mka")

                ts = start_dt.strftime("%Y%m%d_%H%M%S")
                out_choice = self.output_format.get()
                out_ext = ".mp3" if "MP3" in out_choice else ".m4a"
                final_file = os.path.join(out_dir, f"{prefix}_{ts}_{i:03d}{out_ext}")

                def _ui_start():
                    self.chunk_progress_text.set(f"Chunk: {i}/{num_chunks}")
                    self.time_progress_text.set(f"Time: 00:00 / {self.fmt_mmss(dur)}")
                    self.pb_chunk.configure(maximum=max(1, dur))
                    self.pb_chunk["value"] = 0
                    self.pb_total["value"] = i - 1
                self.root.after(0, _ui_start)

                self.log(f"CAPTURE {i}/{num_chunks}: {dur}s | album='{station}' | title='{title_range}'")

                # IMPORTANT FIXES:
                # -re forces ffmpeg to read in realtime (prevents instant buffered dump / repeated same audio)
                # pacing sleep below guarantees chunk spacing even if ffmpeg finishes early
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
                            self.time_progress_text.set(f"Time: {self.fmt_mmss(e2)} / {self.fmt_mmss(dur)}")
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
                self.current_process = None

                # pacing: even if ffmpeg finished early, don't start next chunk early
                elapsed_wall = time.time() - start_wall
                if (not self.stop_requested) and (elapsed_wall < dur):
                    time.sleep(dur - elapsed_wall)

                # Force progress full only if capture succeeded
                def _ui_full():
                    self.pb_chunk["value"] = dur
                    self.time_progress_text.set(f"Time: {self.fmt_mmss(dur)} / {self.fmt_mmss(dur)}")

                if (not os.path.exists(temp_file)) or (os.path.getsize(temp_file) < 20_000):
                    tail = (err or "").strip().splitlines()[-10:]
                    if tail:
                        self.log("CAPTURE FAILED: ffmpeg stderr tail:")
                        for line in tail:
                            self.log("  " + line[:300])
                    else:
                        self.log("CAPTURE FAILED: produced no/too-small file (no stderr captured).")

                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except:
                        pass
                    continue

                self.root.after(0, _ui_full)

                job = {
                    "i": i,
                    "num_chunks": num_chunks,
                    "dur": dur,
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

        except Exception as e:
            self.log(f"CAPTURE CRITICAL ERROR: {e}")

        finally:
            self.job_q.put(None)

    # -------------------- Thread B: PROCESS ONLY --------------------
    def worker_process(self):
        try:
            ffmpeg = self.ffmpeg_path.get().strip()
            fade_sec = self.safe_int(self.fade_duration.get(), default=0)

            self.log("PROCESSOR: ready.")
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

                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                try:
                    os.remove(temp_file)
                except:
                    pass

                self.log(f"SAVED: {os.path.basename(final_file)}")
                self.root.after(0, lambda v=i: self.pb_total.configure(value=v))

            self.log("PROCESSOR: finished.")

        except Exception as e:
            self.log(f"PROCESS CRITICAL ERROR: {e}")

        finally:
            self.is_running = False
            self.current_process = None
            self.root.after(0, self.reset_buttons)

# -------------------- main --------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = StroadGold(root)
    root.mainloop()
