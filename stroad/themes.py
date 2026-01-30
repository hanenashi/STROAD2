from tkinter import ttk

THEMES = {
    "Dark": {
        "bg": "#2b2b2b",
        "panel": "#333333",
        "field": "#3a3a3a",
        "text": "#e6e6e6",
        "accent": "#00ff00",
        "border": "#444444",
    },
    "Light": {
        "bg": "#f3f3f3",
        "panel": "#ffffff",
        "field": "#ffffff",
        "text": "#111111",
        "accent": "#006600",
        "border": "#c8c8c8",
    },
    "System": None,  # don't override ttk theme/colors
}

def apply_theme(root, theme_name: str):
    """Apply a theme. For 'System' we keep ttk defaults as much as possible."""
    style = ttk.Style(root)

    if theme_name == "System":
        # Let system/ttk defaults shine; still pick a usable theme if available.
        try:
            style.theme_use("aqua")  # macOS
        except Exception:
            try:
                style.theme_use("default")
            except Exception:
                pass
        root.configure(bg=None)
        return THEMES["Dark"]  # fallback for non-ttk widgets (log area etc.)

    palette = THEMES.get(theme_name) or THEMES["Dark"]

    root.configure(bg=palette["bg"])
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(".", background=palette["bg"], foreground=palette["text"])
    style.configure("TFrame", background=palette["bg"])
    style.configure("TLabelframe", background=palette["bg"], foreground=palette["text"])
    style.configure("TLabelframe.Label", background=palette["bg"], foreground=palette["text"])
    style.configure("TLabel", background=palette["bg"], foreground=palette["text"])

    style.configure("TButton", background=palette["panel"], foreground=palette["text"], bordercolor=palette["border"])
    style.map(
        "TButton",
        background=[("active", "#3d3d3d"), ("disabled", palette["bg"])],
        foreground=[("disabled", "#888888")]
    )

    style.configure(
        "TEntry",
        fieldbackground=palette["field"],
        foreground=palette["text"],
        background=palette["panel"],
        bordercolor=palette["border"],
        insertcolor=palette["text"]
    )
    style.configure(
        "TCombobox",
        fieldbackground=palette["field"],
        foreground=palette["text"],
        background=palette["panel"],
        bordercolor=palette["border"],
        arrowcolor=palette["text"]
    )
    style.map(
        "TCombobox",
        fieldbackground=[("readonly", palette["field"])],
        background=[("readonly", palette["panel"])],
        foreground=[("readonly", palette["text"])]
    )

    style.configure("Horizontal.TProgressbar", background="#666666", troughcolor=palette["field"])
    return palette
