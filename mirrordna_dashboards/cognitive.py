#!/usr/bin/env python3
"""
MirrorDNA Cognitive Dashboard
Terminal UI — the factory's awareness layer.
Reads from: CONTINUITY.md, system_health, bus, metabolism, PULSE, identity_drift
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import re
import urllib.request

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.live import Live
    from rich.columns import Columns
    from rich.align import Align
    from rich import box
except ImportError:
    print("pip install rich")
    sys.exit(1)

HOME = Path.home()
MIRRORDNA = HOME / ".mirrordna"
BUS = MIRRORDNA / "bus"
VAULT = HOME / "MirrorDNA-Vault"
CLAUDE_DIR = HOME / ".claude"
CC_EVENTS = BUS / "cc_events.jsonl"
FACTORY_DIR = Path("/tmp/mirror-factory")
COUNCIL_DIR = MIRRORDNA / "council"

console = Console()

# ─── Theme ───────────────────────────────────────────────────────────────────

BRAND = "bright_cyan"
CLR_COUNCIL = "plum1"
CLR_OK = "green"
CLR_WARN = "yellow"
CLR_ERR = "bright_red"
CLR_DIM = "grey70"
CLR_PAUL = "orchid1"
CLR_BUS = "steel_blue1"
CLR_FACTORY_ACTIVE = "cyan"
CLR_FACTORY_DONE = "green"
CLR_NEXT = "chartreuse3"
CLR_LOOPS = "gold1"
CLR_SESSION = "bright_white"
CLR_EVO = "medium_spring_green"

# Pulse animation frames (cycles every refresh)
_PULSE_FRAMES = ["◇", "◆", "⟡", "◈", "⟡", "◆"]
_pulse_idx = 0

# Personality — mood-reactive greetings
_GREETINGS = {
    "morning_low": "quiet morning",
    "morning_high": "engines hot",
    "day_low": "standing by",
    "day_high": "in the zone",
    "evening_low": "winding down",
    "evening_high": "burning midnight oil",
    "night": "the machine dreams",
}


def load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return {}


STALENESS_THRESHOLD = 7200
REPOS_DIR = Path.home() / "repos"
HANDOFF_DIR = MIRRORDNA / "handoff"

_state_cache = {"data": None, "ts": 0}
_STATE_CACHE_TTL = 25

# Track service down states to send notifications only once
_service_alerts = {"ollama_down": False}


# ─── Visual helpers ──────────────────────────────────────────────────────────

def _bar(value, max_val, width=16, fill_color="cyan", empty_color="bright_black"):
    """Render a gradient progress bar."""
    if max_val <= 0:
        return f"[{empty_color}]{'━' * width}[/]"
    ratio = min(value / max_val, 1.0)
    filled = int(ratio * width)
    empty = width - filled

    # Gradient: low=green, mid=yellow, high=red for cost/drift
    if fill_color == "gradient":
        if ratio < 0.3:
            fill_color = "green"
        elif ratio < 0.6:
            fill_color = "yellow"
        else:
            fill_color = "red"

    bar = f"[{fill_color}]{'━' * filled}[/][{empty_color}]{'━' * empty}[/]"
    return bar


def _colored_sparkline(data, width=14):
    """Sparkline with per-bar coloring based on value."""
    if not data or max(data) == 0:
        return f"[{CLR_DIM}]" + "▁" * width + "[/]"

    blocks = "▁▂▃▄▅▆▇█"
    mx = max(data)

    if len(data) > width:
        step = len(data) / width
        sampled = [data[int(i * step)] for i in range(width)]
    else:
        sampled = data + [0] * (width - len(data))

    chars = []
    for v in sampled[:width]:
        idx = min(int(v / mx * (len(blocks) - 1)), len(blocks) - 1) if mx > 0 else 0
        ratio = v / mx if mx > 0 else 0
        if ratio > 0.7:
            color = "bright_cyan"
        elif ratio > 0.4:
            color = "cyan"
        elif ratio > 0:
            color = "steel_blue1"
        else:
            color = CLR_DIM
        chars.append(f"[{color}]{blocks[idx]}[/]")
    return "".join(chars)


def _dot(ok):
    """Status dot."""
    return f"[{CLR_OK}]●[/]" if ok else f"[{CLR_ERR}]●[/]"


def _rule(text="", color=CLR_DIM):
    """Inline styled rule/separator."""
    if text:
        pad = "─" * 2
        return f"[{color}]{pad} {text} {pad}[/]"
    return f"[{color}]{'─' * 20}[/]"


def _scanline(width=20):
    """CRT scanline separator."""
    return f"[bright_black]{'░' * width}[/]"


# ─── Data gatherers (unchanged) ─────────────────────────────────────────────

def _parse_continuity():
    path = MIRRORDNA / "CONTINUITY.md"
    if not path.exists():
        return {"sync": "?", "phase": "?", "energy": "?", "next": [], "loops": [], "emotional": ""}
    text = path.read_text()
    lines = text.split("\n")
    result = {"sync": "?", "phase": "?", "energy": "?", "emotional": "", "next": [], "loops": []}
    for line in lines:
        if line.startswith("> Last sync:"):
            result["sync"] = line.replace("> Last sync:", "").strip()
            break
    in_paul = in_next = in_loops = False
    for line in lines:
        if line.startswith("## Paul right now"):
            in_paul, in_next, in_loops = True, False, False; continue
        elif line.startswith("## What's next"):
            in_paul, in_next, in_loops = False, True, False; continue
        elif line.startswith("## Open loops"):
            in_paul, in_next, in_loops = False, False, True; continue
        elif line.startswith("## ") and (in_paul or in_next or in_loops):
            in_paul = in_next = in_loops = False; continue
        if in_paul:
            if line.startswith("**Phase:**"):
                result["phase"] = line.replace("**Phase:**", "").strip()
            elif line.startswith("**Energy:**"):
                result["energy"] = line.replace("**Energy:**", "").strip()
            elif line.startswith("**Emotional thread:**"):
                result["emotional"] = line.replace("**Emotional thread:**", "").strip()
        if in_next and line.strip().startswith(("1.", "2.", "3.", "4.", "5.", "6.")):
            item = line.strip()
            if "**" in item:
                parts = item.split("**")
                if len(parts) >= 3:
                    item = parts[1] + " " + parts[2].lstrip(":").strip()[:50]
            result["next"].append(_strip_md(item)[:50])
        if in_loops and line.strip().startswith("- ["):
            stripped = line.strip()
            # Extract status and item text from "- [x] text" or "- [parked] text"
            import re as _re
            m = _re.match(r"^- \[([^\]]*)\]\s*(.*)", stripped)
            if m:
                status_tag = m.group(1).strip()
                item = m.group(2).strip()
                check = (status_tag == "x")
            else:
                check = False
                item = stripped[5:].strip()
            result["loops"].append(("done" if check else "open", item))
    return result


def _continuity_age_hours():
    path = MIRRORDNA / "CONTINUITY.md"
    if not path.exists():
        return None
    return (time.time() - path.stat().st_mtime) / 3600


def _infer_phase():
    hour = datetime.now().hour
    active_repos = []
    active_areas = []
    now = time.time()
    one_hour = 3600

    # 1. Git repos with recent commits
    try:
        result = subprocess.run(
            ["bash", "-c",
             "for d in ~/repos/*/; do cd \"$d\" 2>/dev/null && "
             "count=$(git log --oneline --since='1 hour ago' 2>/dev/null | wc -l | tr -d ' '); "
             "[ \"$count\" -gt 0 ] && echo \"$(basename $d):$count\"; done"],
            capture_output=True, text=True, timeout=15)
        for line in result.stdout.strip().split("\n"):
            if line.strip():
                active_repos.append(line.strip())
    except Exception:
        pass

    # 2. Claude Code session events (cc_events.jsonl) — what Claude is actually working on
    cc_events = MIRRORDNA / "bus" / "cc_events.jsonl"
    if cc_events.exists():
        try:
            lines = cc_events.read_text().strip().split("\n")
            for line in reversed(lines[-20:]):
                try:
                    ev = json.loads(line)
                    ts = ev.get("timestamp", "")
                    if ts:
                        from datetime import timezone
                        evt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if (datetime.now(timezone.utc) - evt).total_seconds() < one_hour:
                            desc = ev.get("description", ev.get("name", ""))
                            if desc and desc not in active_areas:
                                active_areas.append(desc)
                except Exception:
                    continue
        except Exception:
            pass

    # 3. Recently modified vault folders (what's being organized/written)
    vault = Path.home() / "MirrorDNA-Vault"
    active_dirs = ["01_ACTIVE", "00_INBOX", "00_Dashboard"]
    for d in active_dirs:
        check = vault / d
        if check.exists():
            for item in check.iterdir():
                if item.is_dir() and (now - item.stat().st_mtime) < one_hour:
                    active_areas.append(item.name)

    # 4. Recently modified mirrordna scripts/configs
    for scan in [MIRRORDNA / "scripts", MIRRORDNA / "mcp", MIRRORDNA / "overnight"]:
        if scan.exists():
            for f in scan.iterdir():
                if f.is_file() and (now - f.stat().st_mtime) < one_hour:
                    active_areas.append(f.stem)
                    break  # one per dir is enough

    recent_files = 0
    for scan_dir in [HANDOFF_DIR, MIRRORDNA / "staging"]:
        if scan_dir.exists():
            for f in scan_dir.iterdir():
                if f.is_file() and (now - f.stat().st_mtime) < one_hour:
                    recent_files += 1

    has_activity = len(active_repos) > 0 or recent_files > 0 or len(active_areas) > 0

    # Build activity string from all sources
    all_active = [r.split(":")[0] for r in active_repos[:3]]
    for a in active_areas[:3]:
        if a not in all_active:
            all_active.append(a)
    all_active = all_active[:4]  # cap at 4 items

    if 6 <= hour < 10:
        if has_activity:
            return f"Morning — active: {', '.join(all_active)}" if all_active else "Morning — early start"
        return "Morning routine"
    elif 10 <= hour < 22:
        if has_activity:
            items = ", ".join(all_active) if all_active else "multiple areas"
            return f"Building. Active on: {items}"
        return "Work hours — quiet" if hour < 17 else "Evening wind-down"
    return "Away or resting"


def _infer_energy():
    count = 0
    for scan_dir in [HANDOFF_DIR, MIRRORDNA / "staging", MIRRORDNA / "bus"]:
        if scan_dir.exists():
            for f in scan_dir.iterdir():
                if f.is_file() and (time.time() - f.stat().st_mtime) < 3600:
                    count += 1
    try:
        result = subprocess.run(
            ["bash", "-c",
             "for d in ~/repos/*/; do cd \"$d\" 2>/dev/null && "
             "git log --oneline --since='1 hour ago' 2>/dev/null; done | wc -l"],
            capture_output=True, text=True, timeout=15)
        count += int(result.stdout.strip())
    except Exception:
        pass
    if count > 10:
        return "High"
    elif count > 3:
        return "Medium"
    return "Low"


def _strip_md(text):
    text = re.sub(r'~~.*?~~', '', text)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'[^\x00-\x7F\u2014\u2013\u2019\u2018\u201c\u201d]+', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def _infer_next_items():
    if not HANDOFF_DIR.exists():
        return []
    handoffs = sorted(HANDOFF_DIR.glob("HO-*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not handoffs:
        return []
    priority_headers = ("## next", "## what's next", "## todo", "## tasks",
                        "## build order", "## what we build")
    fallback_headers = ("## problem", "## the real problem", "## fix")
    for handoff_path in handoffs[:3]:
        try:
            text = handoff_path.read_text()
        except Exception:
            continue
        for header_set in (priority_headers, fallback_headers):
            items = _extract_list_items(text, header_set)
            if items:
                return items
    return []


def _extract_list_items(text, header_keywords):
    items = []
    in_section = False
    for line in text.split("\n"):
        lower = line.strip().lower()
        if any(lower.startswith(kw) for kw in header_keywords):
            in_section = True; continue
        elif line.startswith("## ") and in_section:
            break
        if in_section and line.strip():
            raw = line.strip()
            if not (raw.startswith("-") or raw.startswith("*") or
                    (len(raw) > 1 and raw[0].isdigit() and raw[1] in ".)")):
                continue
            if raw.startswith("|") or raw.startswith("---") or raw.startswith("  "):
                continue
            cleaned = _strip_md(raw.lstrip("-*0123456789.) "))
            if cleaned and len(cleaned) > 5:
                items.append(cleaned[:60])
            if len(items) >= 5:
                break
    return items


def _get_bus_pending():
    pending = []
    wo_dir = BUS / "workorders"
    if wo_dir.exists():
        for f in wo_dir.glob("WO-*.json"):
            try:
                wo = json.loads(f.read_text())
                if wo.get("status") not in ("completed", "cancelled"):
                    pending.append(("open", f"WO: {wo.get('title', f.stem)[:50]}"))
            except Exception:
                pass
    prop_dir = BUS / "proposals"
    if prop_dir.exists():
        for f in prop_dir.glob("PROP-*.md"):
            try:
                text = f.read_text()
                if "status: resolved" not in text and "status: rejected" not in text:
                    for line in text.split("\n"):
                        if line.startswith("# "):
                            pending.append(("open", f"PROP: {line[2:].strip()[:50]}"))
                            break
            except Exception:
                pass
    return pending


def _ollama_status():
    try:
        req = urllib.request.Request("http://localhost:11434/api/ps", method="GET")
        with urllib.request.urlopen(req, timeout=2) as resp:
            data = json.loads(resp.read())
            models = data.get("models", [])
            if not models:
                return {"running": True, "model": None}
            m = models[0]
            size_gb = m.get("size_vram", m.get("size", 0)) / (1024**3)
            return {
                "running": True, "model": m.get("name", "?").split(":")[0],
                "params": m.get("details", {}).get("parameter_size", "?"),
                "vram_gb": round(size_gb, 1),
                "ctx": m.get("context_length", "?"),
                "quant": m.get("details", {}).get("quantization_level", "?"),
            }
    except Exception:
        return {"running": False, "model": None}


def _cc_session_tokens():
    debug_latest = CLAUDE_DIR / "debug" / "latest"
    session_id = None
    if debug_latest.exists():
        try:
            target = debug_latest.resolve()
            for part in target.parts:
                if len(part) == 36 and part.count("-") == 4:
                    session_id = part; break
        except Exception:
            pass
    if not session_id:
        project_dir = CLAUDE_DIR / "projects" / "-Users-mirror-admin-MirrorDNA-Vault"
        if project_dir.exists():
            jsonls = sorted(project_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
            if jsonls:
                session_id = jsonls[0].stem
    if not session_id:
        return None
    jsonl_path = CLAUDE_DIR / "projects" / "-Users-mirror-admin-MirrorDNA-Vault" / f"{session_id}.jsonl"
    if not jsonl_path.exists():
        return None
    ti = to = cr = cc_val = 0
    msg_count = 0
    try:
        with open(jsonl_path) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    if d.get("type") == "assistant":
                        u = d.get("message", {}).get("usage", {})
                        if u:
                            ti += u.get("input_tokens", 0)
                            to += u.get("output_tokens", 0)
                            cr += u.get("cache_read_input_tokens", 0)
                            cc_val += u.get("cache_creation_input_tokens", 0)
                            msg_count += 1
                except (json.JSONDecodeError, KeyError):
                    pass
    except Exception:
        return None
    return {
        "session_id": session_id[:8], "input_tokens": ti, "output_tokens": to,
        "cache_read": cr, "cache_create": cc_val, "messages": msg_count,
        "total": ti + to + cr + cc_val,
    }


def _recent_cc_events(minutes=5):
    if not CC_EVENTS.exists():
        return []
    cutoff = time.time() - (minutes * 60)
    events = []
    try:
        with open(CC_EVENTS) as f:
            for line in f:
                try:
                    ev = json.loads(line)
                    if ev.get("epoch", 0) > cutoff:
                        events.append(ev)
                except (json.JSONDecodeError, KeyError):
                    pass
    except Exception:
        pass
    return events


def _launchagent_health():
    try:
        result = subprocess.run(["launchctl", "list"], capture_output=True, text=True, timeout=5)
        agents = {}
        for line in result.stdout.split("\n"):
            if "mirror" in line.lower() or "activemirror" in line.lower():
                parts = line.split("\t")
                if len(parts) >= 3:
                    pid, status, label = parts[0].strip(), parts[1].strip(), parts[2].strip()
                    is_crash = False
                    try:
                        is_crash = int(status) > 0 and pid == '-'
                    except (ValueError, TypeError):
                        pass
                    agents[label.split(".")[-1]] = {"pid": pid, "exit": status, "crashed": is_crash}
        return agents
    except Exception:
        return {}


def _bus_writes_per_hour():
    changelog = BUS / "changelog.jsonl"
    if not changelog.exists():
        return {"per_hour": 0, "writers": {}, "spark_24h": [0]*24}
    one_h = time.time() - 3600
    twenty_four_h = time.time() - 86400
    recent_count = 0
    writers = {}
    buckets = {}
    try:
        with open(changelog) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    ts_str = entry.get("timestamp", "")
                    writer = entry.get("writer", "?")
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    epoch = dt.timestamp()
                    if epoch > one_h:
                        recent_count += 1
                        writers[writer] = writers.get(writer, 0) + 1
                    if epoch > twenty_four_h:
                        buckets[dt.strftime("%H")] = buckets.get(dt.strftime("%H"), 0) + 1
                except (json.JSONDecodeError, ValueError):
                    pass
    except Exception:
        pass
    return {"per_hour": recent_count, "writers": writers,
            "spark_24h": [buckets.get(f"{h:02d}", 0) for h in range(24)]}


def _continuity_diff():
    if not (MIRRORDNA / "CONTINUITY.md").exists():
        return None
    try:
        result = subprocess.run(
            ["git", "-C", str(MIRRORDNA), "diff", "--stat", "HEAD~1", "--", "CONTINUITY.md"],
            capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().split("\n")[-1].strip()
    except Exception:
        pass
    return None


_vault_cache = {"data": None, "ts": 0}
_VAULT_CACHE_TTL = 60


def _vault_activity_today():
    now = time.time()
    if _vault_cache["data"] and (now - _vault_cache["ts"]) < _VAULT_CACHE_TTL:
        return _vault_cache["data"]
    today_start = datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    created = modified = 0
    projects = set()
    for scan_dir in [VAULT / "01_ACTIVE", VAULT / "00_INBOX", VAULT / "SessionReports"]:
        if not scan_dir.exists():
            continue
        try:
            for f in scan_dir.rglob("*"):
                if not f.is_file():
                    continue
                try:
                    st = f.stat()
                    if st.st_birthtime >= today_start:
                        created += 1
                        if "01_ACTIVE" in str(f):
                            try:
                                projects.add(f.relative_to(VAULT / "01_ACTIVE").parts[0])
                            except (ValueError, IndexError):
                                pass
                    elif st.st_mtime >= today_start:
                        modified += 1
                except (OSError, AttributeError):
                    pass
        except Exception:
            pass
    result = {"created": created, "modified": modified, "projects": list(projects)[:5]}
    _vault_cache["data"] = result
    _vault_cache["ts"] = now
    return result


def _inbox_anomaly():
    meta = load_json(BUS / "metabolism.json")
    history = meta.get("history", [])
    if len(history) < 2:
        return None
    earliest, latest = history[0], history[-1]
    try:
        t0 = datetime.fromisoformat(earliest["timestamp"].replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(latest["timestamp"].replace("Z", "+00:00"))
        hours = (t1 - t0).total_seconds() / 3600
        if hours < 1:
            return None
        n0, n1 = earliest.get("total_notes", 0), latest.get("total_notes", 0)
        if n1 > n0 + 50:
            return f"+{n1 - n0} in {hours:.0f}h"
    except Exception:
        pass
    return None


def get_paul_state():
    now = time.time()
    if _state_cache["data"] and (now - _state_cache["ts"]) < _STATE_CACHE_TTL:
        return _state_cache["data"]
    cached = _parse_continuity()
    age_h = _continuity_age_hours()
    if age_h is not None and (age_h * 3600) < STALENESS_THRESHOLD:
        # File is fresh by mtime, but energy/phase may be stale
        # (hooks touch mtime without updating content).
        # Always blend in live inference for energy — it's objectively measurable.
        live_energy = _infer_energy()
        continuity_energy = cached.get("energy", "")
        # Override if live signal contradicts continuity
        if live_energy == "High" and "High" not in continuity_energy:
            cached["energy"] = f"{live_energy} — active session"
        elif live_energy == "Low" and ("High" in continuity_energy or "Building" in continuity_energy):
            cached["energy"] = f"{live_energy} — quiet"
        # Also blend phase if git activity shows current work
        live_phase = _infer_phase()
        if "Active on:" in live_phase:
            cached["phase"] = live_phase
        cached["source"] = "continuity"
        cached["age_hours"] = age_h
        bus_pending = _get_bus_pending()
        existing = {item for _, item in cached.get("loops", [])}
        for s, i in bus_pending:
            if i not in existing:
                cached["loops"].append((s, i))
        _state_cache["data"] = cached
        _state_cache["ts"] = now
        return cached
    loops = cached.get("loops", [])
    bus_pending = _get_bus_pending()
    existing = {item for _, item in loops}
    for s, i in bus_pending:
        if i not in existing:
            loops.append((s, i))
    continuity_next = cached.get("next", [])
    if continuity_next:
        next_items, next_source = continuity_next, "continuity"
    else:
        handoff_items = _infer_next_items()
        next_items = handoff_items
        next_source = "handoff" if handoff_items else "none"
    result = {
        "source": "live", "next_source": next_source, "age_hours": age_h,
        "phase": _infer_phase(), "energy": _infer_energy(),
        "emotional": cached.get("emotional", ""), "sync": cached.get("sync", "?"),
        "next": next_items, "loops": loops,
    }
    _state_cache["data"] = result
    _state_cache["ts"] = now
    return result


def _find_active_factory_run():
    if not FACTORY_DIR.exists():
        return None
    runs = [d for d in FACTORY_DIR.iterdir() if d.is_dir() and (d / "orchestration.jsonl").exists()]
    if not runs:
        return None
    runs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    if (time.time() - runs[0].stat().st_mtime) > 21600:
        return None
    return runs[0]


# ─── Panel renderers ────────────────────────────────────────────────────────

def header_panel():
    """Branded header with live metrics bar."""
    global _pulse_idx
    _pulse_idx = (_pulse_idx + 1) % len(_PULSE_FRAMES)
    pulse_char = _PULSE_FRAMES[_pulse_idx]

    now = datetime.now().strftime("%H:%M")
    bh = load_json(BUS / "health.json")
    header_data = load_json(BUS / "header.json")
    coherence = load_json(BUS / "identity_coherence.json")
    drift = load_json(BUS / "identity_drift.json")

    # Bus
    sv = header_data.get("state_version", bh.get("state_version", "?"))
    bus_ok = bh.get("status") == "healthy"

    # Coherence (3-dimensional) — falls back to old drift if coherence not available
    coh_cur = coherence.get("current", {})
    composite = coh_cur.get("composite_score", 0)
    coh_state = coh_cur.get("state", "")
    if composite > 0:
        d_pct = 100 - composite  # invert: 94% coherence = 6% "drift" for the bar
        dc = CLR_OK if composite >= 90 else CLR_WARN if composite >= 75 else CLR_ERR
        drift_bar = _bar(d_pct, 30, width=8, fill_color=dc)
    else:
        drift_history = drift.get("history", [])
        drift_val = drift.get("latest_drift")
        if drift_val is None and drift_history:
            drift_val = drift_history[-1].get("drift_score")
        d_pct = (float(drift_val) * 100 if drift_val and float(drift_val) < 1 else float(drift_val or 0))
        composite = 100 - d_pct
        coh_state = "legacy"
        dc = CLR_OK if d_pct < 2 else CLR_WARN if d_pct < 5 else CLR_ERR
        drift_bar = _bar(d_pct, 15, width=8, fill_color=dc)

    # Cost
    tokens = _cc_session_tokens()
    cost_str = ""
    if tokens and tokens["total"] > 0:
        cost = (tokens["input_tokens"] * 15 + tokens["output_tokens"] * 75 +
                tokens["cache_read"] * 1.875) / 1_000_000
        cc_color = CLR_OK if cost < 1 else CLR_WARN if cost < 5 else CLR_ERR
        cost_bar = _bar(cost, 50, width=6, fill_color="gradient")
        cost_str = f"  [{cc_color}]${cost:.2f}[/] {cost_bar}"

    # Factory status inline
    factory_str = ""
    run_dir = _find_active_factory_run()
    if run_dir:
        statuses = [sf.read_text().strip() for sf in run_dir.glob("*.status")]
        running = statuses.count("running")
        done = statuses.count("done")
        total = len(statuses)
        if running > 0:
            factory_str = f"  [{CLR_FACTORY_ACTIVE} bold]FACTORY {done}/{total}[/]"
        elif done == total and total > 0:
            factory_str = f"  [{CLR_FACTORY_DONE}]FACTORY {total}/{total}[/]"

    # Personality — time + energy aware greeting
    hour = datetime.now().hour
    energy = _infer_energy()
    if hour < 6:
        mood = _GREETINGS["night"]
    elif hour < 10:
        mood = _GREETINGS["morning_high"] if energy == "High" else _GREETINGS["morning_low"]
    elif hour < 20:
        mood = _GREETINGS["day_high"] if energy != "Low" else _GREETINGS["day_low"]
    else:
        mood = _GREETINGS["evening_high"] if energy == "High" else _GREETINGS["evening_low"]

    metrics_line = (
        f"[{BRAND} bold]{pulse_char} MIRRORDNA[/]  "
        f"[grey70]{now}[/]  "
        f"Bus:{_dot(bus_ok)}[grey70]v{sv}[/]  "
        f"Coherence:[{dc}]{composite:.0f}%[/] {drift_bar}"
        f"{cost_str}{factory_str}"
    )
    mood_line = f"  [{CLR_PAUL} bold italic]// {mood}[/]"

    header_border = f"{CLR_ERR} blink" if composite < 60 else BRAND
    return Panel(
        Text.from_markup(f"{metrics_line}\n{mood_line}"),
        border_style=header_border, box=box.HEAVY_EDGE, padding=(0, 1),
    )


def _system_vitals():
    """Get CPU, RAM, disk, memory pressure."""
    vitals = {}
    try:
        # Memory
        result = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=3)
        lines = result.stdout.strip().split("\n")
        page_size = 16384  # Apple Silicon default
        free = active = inactive = wired = compressed = 0
        for line in lines:
            if "Pages free:" in line:
                free = int(line.split(":")[1].strip().rstrip(".")) * page_size
            elif "Pages active:" in line:
                active = int(line.split(":")[1].strip().rstrip(".")) * page_size
            elif "Pages inactive:" in line:
                inactive = int(line.split(":")[1].strip().rstrip(".")) * page_size
            elif "Pages wired" in line:
                wired = int(line.split(":")[1].strip().rstrip(".")) * page_size
            elif "Pages occupied by compressor:" in line:
                compressed = int(line.split(":")[1].strip().rstrip(".")) * page_size
        total_bytes = 32 * 1024**3  # 32GB M4 Mac mini
        used = active + wired + compressed
        vitals["ram_used_gb"] = round(used / 1024**3, 1)
        vitals["ram_total_gb"] = 32
        vitals["ram_pct"] = round(used / total_bytes * 100)
        vitals["ram_pressure"] = "nominal" if vitals["ram_pct"] < 70 else "warning" if vitals["ram_pct"] < 85 else "critical"
    except Exception:
        pass

    try:
        # CPU
        result = subprocess.run(
            ["bash", "-c", "top -l 1 -n 0 | grep 'CPU usage'"],
            capture_output=True, text=True, timeout=5)
        m = re.search(r'(\d+\.\d+)% user.*?(\d+\.\d+)% sys.*?(\d+\.\d+)% idle', result.stdout)
        if m:
            vitals["cpu_user"] = float(m.group(1))
            vitals["cpu_sys"] = float(m.group(2))
            vitals["cpu_idle"] = float(m.group(3))
            vitals["cpu_pct"] = round(vitals["cpu_user"] + vitals["cpu_sys"])
    except Exception:
        pass

    try:
        # Disk
        result = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=3)
        parts = result.stdout.strip().split("\n")[-1].split()
        vitals["disk_used"] = parts[2]
        vitals["disk_avail"] = parts[3]
        vitals["disk_pct"] = int(parts[4].rstrip("%"))
    except Exception:
        pass

    return vitals


def infra_panel():
    """Services + ports + vitals — reads from SERVICE_REGISTRY.json."""
    registry = _load_service_registry()
    la_health = _launchagent_health()

    lines = []

    # System vitals first
    v = _system_vitals()
    if v:
        cpu = v.get("cpu_pct", 0)
        ram = v.get("ram_pct", 0)
        disk = v.get("disk_pct", 0)
        cpu_c = CLR_OK if cpu < 50 else CLR_WARN if cpu < 80 else CLR_ERR
        ram_c = CLR_OK if ram < 70 else CLR_WARN if ram < 85 else CLR_ERR
        disk_c = CLR_OK if disk < 75 else CLR_WARN if disk < 90 else CLR_ERR
        cpu_bar = _bar(cpu, 100, width=6, fill_color=cpu_c)
        ram_bar = _bar(ram, 100, width=6, fill_color=ram_c)
        lines.append(f"  CPU [{cpu_c}]{cpu}%[/] {cpu_bar}  RAM [{ram_c}]{v.get('ram_used_gb',0)}G[/] {ram_bar}")
        lines.append(f"  Disk [{disk_c}]{v.get('disk_used','?')}/{v.get('disk_avail','?')}[/] [{CLR_DIM}]{disk}%[/]")

    lines.append("")

    # Ports from registry (single source of truth)
    try:
        result = subprocess.run(
            ["lsof", "-iTCP", "-sTCP:LISTEN", "-P", "-n"],
            capture_output=True, text=True, timeout=5)
        listening = result.stdout
    except Exception:
        listening = ""

    svc_up = 0
    svc_total = 0
    down_names = []
    for svc in registry.get("services", []):
        port = svc.get("port")
        if not port:
            continue
        svc_total += 1
        if _check_port(port, listening):
            svc_up += 1
        else:
            down_names.append(svc["name"][:8])

    port_bar = _bar(svc_up, svc_total, width=10, fill_color=CLR_OK if svc_up == svc_total else CLR_WARN)
    lines.append(f"  Ports {port_bar} {svc_up}/{svc_total}")

    if down_names:
        lines.append(f"  [{CLR_ERR}]{', '.join(down_names[:4])}[/]")
    elif svc_up == svc_total:
        lines.append(f"  [{CLR_OK} bold]ALL SYSTEMS NOMINAL[/]")

    infra_border = CLR_BUS if svc_up == svc_total else f"{CLR_ERR} blink" if len(down_names) > 3 else CLR_WARN
    return Panel(
        "\n".join(lines), title=f"[{CLR_BUS}]Infrastructure[/]",
        border_style=infra_border, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def vitals_panel():
    """Bus + vault vitals with visual bars."""
    bh = load_json(BUS / "health.json")
    header_data = load_json(BUS / "header.json")
    pulse = load_json(MIRRORDNA / "organism" / "PULSE.json")
    drift = load_json(BUS / "identity_drift.json")
    meta = load_json(BUS / "metabolism.json")

    lines = []

    # Bus health bar
    sv = header_data.get("state_version", bh.get("state_version", "?"))
    integrity = header_data.get("write_complete", bh.get("checksum_valid", False))
    lines.append(f"  [{CLR_BUS} bold]BUS[/] v{sv} {'[green]■[/]' if integrity else '[red]■ BROKEN[/]'}")

    # Pulse
    seq = pulse.get("beat", {}).get("sequence", "?")
    lines.append(f"  [{CLR_DIM}]Pulse #{seq}[/]")

    # Drift with bar + sparkline
    drift_history = drift.get("history", [])
    drift_val = drift.get("latest_drift")
    if drift_val is None and drift_history:
        drift_val = drift_history[-1].get("drift_score")
    if drift_val is not None:
        d = float(drift_val)
        d_pct = d * 100 if d < 1 else d
        dc = CLR_OK if d_pct < 2 else CLR_WARN if d_pct < 5 else CLR_ERR
        drift_bar = _bar(d_pct, 15, width=10, fill_color=dc)
        lines.append(f"  Drift [{dc}]{d_pct:.1f}%[/] {drift_bar}")

    if len(drift_history) > 2:
        scores = [(h.get("drift_score", 0) * 100) for h in drift_history[-24:]]
        lines.append(f"  [{CLR_DIM}]{_colored_sparkline(scores, 14)}[/]")

    # Writes/hour
    bus_writes = _bus_writes_per_hour()
    wph = bus_writes["per_hour"]
    wph_color = CLR_OK if wph > 0 else CLR_DIM
    lines.append(f"  W/h [{wph_color}]{wph}[/] {_colored_sparkline(bus_writes['spark_24h'], 14)}")

    # Write age
    write_ts = header_data.get("timestamp", bh.get("last_write", ""))
    if write_ts:
        try:
            dt = datetime.fromisoformat(write_ts.replace("Z", "+00:00"))
            hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            ac = CLR_OK if hours < 12 else CLR_WARN if hours < 24 else CLR_ERR
            lines.append(f"  [{CLR_DIM}]Last write [{ac}]{hours:.1f}h[/] ago[/]")
        except Exception:
            pass

    # Vault section
    lines.append(f"  {_scanline(16)}")

    latest = meta.get("latest", {})
    total = latest.get("total_notes", 0)
    active = latest.get("active", 0)
    dormant = latest.get("dormant", 0)
    rate = latest.get("metabolism_rate", 0)

    rate_bar = _bar(rate, 1.0, width=10, fill_color=CLR_OK)
    lines.append(f"  [{CLR_BUS} bold]VAULT[/] {total:,}n")
    lines.append(f"  Rate {rate_bar} {rate*100:.0f}%")
    lines.append(f"  [{CLR_OK}]{active}[/]a [{CLR_WARN}]{dormant}[/]d")

    # Vault today
    vault_today = _vault_activity_today()
    if vault_today["created"] > 0 or vault_today["modified"] > 0:
        parts = []
        if vault_today["created"] > 0:
            parts.append(f"[{CLR_OK}]+{vault_today['created']}[/]")
        if vault_today["modified"] > 0:
            parts.append(f"{vault_today['modified']}mod")
        lines.append(f"  Today {' '.join(parts)}")

    # Inbox
    inbox_total = meta.get("folders", {}).get("00_INBOX", {}).get("total", 0)
    if inbox_total > 0:
        lines.append(f"  [{CLR_WARN}]Inbox {inbox_total}[/]")

    # Mesh
    graph = meta.get("graph_health", {})
    if graph:
        avg = graph.get("avg_degree", graph.get("link_density", 0))
        mc = CLR_OK if avg > 10 else CLR_WARN if avg > 2 else CLR_ERR
        lines.append(f"  [{CLR_DIM}]Mesh avg:[{mc}]{avg:.1f}[/][/]")

    return Panel(
        "\n".join(lines), title=f"[{CLR_BUS}]Vitals[/]",
        border_style=CLR_BUS, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def session_panel():
    """Active AI sessions with token telemetry."""
    SESSION_REPORTS = VAULT / "SessionReports"
    agents = []
    detail_lines = []

    # Claude Code — detect via cc_events (primary), debug mtime, or pgrep
    cc_active = False
    cc_session = None

    # Primary: recent tool events in cc_events.jsonl (most reliable signal)
    recent_events = _recent_cc_events(3)
    if recent_events:
        cc_active = True
        sid = recent_events[-1].get("session_id", "")
        if sid:
            cc_session = sid[:8]

    # Secondary: debug log mtime
    if not cc_active:
        debug_latest = CLAUDE_DIR / "debug" / "latest"
        if debug_latest.exists():
            try:
                target = debug_latest.resolve()
                age_min = (time.time() - target.stat().st_mtime) / 60
                if age_min < 5:
                    cc_active = True
                    cc_session = target.stem[:8]
            except Exception:
                pass

    # Tertiary: process check
    if not cc_active:
        try:
            result = subprocess.run(["pgrep", "-f", "claude"], capture_output=True, text=True, timeout=3)
            if result.returncode == 0 and result.stdout.strip():
                cc_active = True
        except Exception:
            pass

    if cc_active:
        sid = f" [grey70]{cc_session}[/]" if cc_session else ""
        agents.append(f"[{CLR_OK} bold]CC[/]{sid}")
    else:
        debug_latest = CLAUDE_DIR / "debug" / "latest"
        if debug_latest.exists():
            try:
                age_h = (time.time() - debug_latest.resolve().stat().st_mtime) / 3600
                if age_h < 1:
                    agents.append(f"[{CLR_WARN}]CC[/] [{CLR_DIM}]{age_h*60:.0f}m ago[/]")
            except Exception:
                pass

    # Tokens + cost
    tokens = _cc_session_tokens()
    if tokens and tokens["total"] > 0:
        billed_k = (tokens["input_tokens"] + tokens["output_tokens"]) / 1000
        cache_k = (tokens["cache_read"] + tokens["cache_create"]) / 1000
        cost = (tokens["input_tokens"] * 15 + tokens["output_tokens"] * 75 +
                tokens["cache_read"] * 1.875) / 1_000_000
        cc_color = CLR_OK if cost < 1 else CLR_WARN if cost < 5 else CLR_ERR
        cost_bar = _bar(cost, 50, width=12, fill_color="gradient")
        detail_lines.append(f"  [{cc_color} bold]${cost:.2f}[/] {cost_bar}")
        detail_lines.append(
            f"  [{CLR_DIM}]{billed_k:.0f}k billed  {cache_k:.0f}k cached  {tokens['messages']}msg[/]"
        )

    # Recent tools
    events = _recent_cc_events(5)
    if events:
        tool_counts = {}
        for ev in events:
            tool_counts[ev.get("tool", "?")] = tool_counts.get(ev.get("tool", "?"), 0) + 1
        top = sorted(tool_counts.items(), key=lambda x: -x[1])[:4]
        detail_lines.append(f"  [{CLR_DIM}]Tools: {' '.join(f'{t}:{c}' for t, c in top)}[/]")

    # Workers
    try:
        result = subprocess.run(
            ["bash", "-c", "ps aux | grep -E 'claude.*(--print| -p )' | grep -v grep | wc -l"],
            capture_output=True, text=True, timeout=3)
        wc = int(result.stdout.strip())
        if wc > 0:
            agents.append(f"[{CLR_FACTORY_ACTIVE} bold]{wc} workers[/]")
    except Exception:
        pass

    # Desktop
    try:
        result = subprocess.run(
            ["pgrep", "-f", "com.anthropic.claudefordesktop"],
            capture_output=True, text=True, timeout=3)
        if result.returncode == 0 and result.stdout.strip():
            agents.append(f"[{CLR_OK}]Desktop[/]")
    except Exception:
        pass

    # Ollama
    ollama = _ollama_status()
    if ollama["running"]:
        if ollama.get("model"):
            agents.append(f"[{CLR_OK}]Ollama[/]")
            detail_lines.append(
                f"  [{CLR_DIM}]{ollama['model']} {ollama.get('params','')} "
                f"{ollama.get('vram_gb','')}GB[/]"
            )
        else:
            agents.append(f"[{CLR_OK}]Ollama[/] [{CLR_DIM}]idle[/]")

    # Crystals
    today = datetime.now().strftime("%Y-%m-%d")
    if SESSION_REPORTS.exists():
        today_reports = list(SESSION_REPORTS.glob(f"SR-{today}*.md"))
        if today_reports:
            detail_lines.append(f"  [{CLR_DIM}]{len(today_reports)} crystals today[/]")

    # Vault activity
    vault_today = _vault_activity_today()
    if vault_today["projects"]:
        projs = ', '.join(vault_today['projects'][:3])[:35]
        detail_lines.append(f"  [{CLR_DIM}]Active: {projs}[/]")

    table = Table(show_header=False, box=None, padding=(0, 0), expand=True)
    table.add_column("content", no_wrap=False, overflow="fold")

    if agents:
        table.add_row(f"  {' [grey70]|[/] '.join(agents)}")
    else:
        table.add_row(f"  [{CLR_DIM}]No agents[/]")
    for dl in detail_lines:
        table.add_row(dl)

    return Panel(
        table, title=f"[{CLR_SESSION}]Sessions[/]",
        border_style=CLR_SESSION, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def paul_panel():
    """Paul's state — phase, energy, emotional thread."""
    ctx = get_paul_state()

    table = Table(show_header=False, box=None, padding=(0, 0), expand=True)
    table.add_column("content", no_wrap=False, overflow="fold")

    table.add_row(f" [{CLR_PAUL} bold]{ctx['phase'][:35]}[/]")

    # Energy with visual indicator
    energy = ctx['energy'][:15]
    if "High" in energy:
        e_bar = f"[{CLR_OK}]■■■[/][{CLR_DIM}]■■[/]"
    elif "Medium" in energy:
        e_bar = f"[{CLR_WARN}]■■[/][{CLR_DIM}]■■■[/]"
    else:
        e_bar = f"[{CLR_ERR}]■[/][{CLR_DIM}]■■■■[/]"
    table.add_row(f" Energy {e_bar} [{CLR_DIM}]{energy}[/]")

    if ctx.get("emotional"):
        emo = ctx['emotional']
        table.add_row(f" [{CLR_DIM} italic]{emo}[/]")

    # Source
    if ctx["source"] == "live":
        age = ctx.get("age_hours")
        if age is not None:
            table.add_row(f" [{CLR_WARN}]live — {age:.0f}h cached[/]")
    else:
        table.add_row(f" [{CLR_OK}]fresh[/]")

    return Panel(
        table, title=f"[{CLR_PAUL}]Paul[/]",
        border_style=CLR_PAUL, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def factory_panel():
    """Sovereign Factory — orchestration status."""
    run_dir = _find_active_factory_run()

    if run_dir is None:
        return Panel(
            f"  [{CLR_DIM}]Idle[/]",
            title=f"[{CLR_DIM}]Factory[/]", border_style=CLR_DIM,
            box=box.HEAVY_EDGE, padding=(0, 0),
        )

    run_id = run_dir.name
    lines = []

    start_ts = complete_ts = None
    try:
        with open(run_dir / "orchestration.jsonl") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                    if ev.get("event") == "START":
                        start_ts = ev.get("ts")
                    elif ev.get("event") == "COMPLETE":
                        complete_ts = ev.get("ts")
                except (json.JSONDecodeError, KeyError):
                    pass
    except Exception:
        pass

    agents = []
    total = done = running = pending = 0

    for sf in sorted(run_dir.glob("*.status")):
        agent_id = sf.stem
        try:
            status = sf.read_text().strip()
        except Exception:
            status = "?"

        log_file = run_dir / f"{agent_id}.log"
        log_kb = 0
        if log_file.exists():
            try:
                log_kb = log_file.stat().st_size / 1024
            except Exception:
                pass

        total += 1
        if status == "done":
            done += 1
            icon = f"[{CLR_OK}]■[/]"
        elif status == "running":
            running += 1
            icon = f"[{CLR_FACTORY_ACTIVE}]▶[/]"
        else:
            pending += 1
            icon = f"[{CLR_DIM}]○[/]"

        agents.append((agent_id, icon, log_kb))

    # Progress bar
    progress = _bar(done, total, width=14,
                    fill_color=CLR_FACTORY_DONE if done == total else CLR_FACTORY_ACTIVE)

    if complete_ts:
        state_str = f"[{CLR_FACTORY_DONE} bold]COMPLETE[/]"
    elif running > 0:
        state_str = f"[{CLR_FACTORY_ACTIVE} bold]BUILDING[/]"
    else:
        state_str = f"[{CLR_WARN}]PENDING[/]"

    lines.append(f"  {state_str} {progress} {done}/{total}")

    # Elapsed
    if start_ts:
        try:
            t0 = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
            t1 = (datetime.fromisoformat(complete_ts.replace("Z", "+00:00"))
                  if complete_ts else datetime.now(timezone.utc))
            elapsed = int((t1 - t0).total_seconds())
            m, s = divmod(elapsed, 60)
            lines.append(f"  [{CLR_DIM}]{m}m{s:02d}s elapsed[/]")
        except Exception:
            pass

    # Agent list
    for agent_id, icon, log_kb in agents:
        lines.append(f"  {icon} {agent_id:<12} [{CLR_DIM}]{log_kb:.0f}k[/]")

    border = CLR_FACTORY_DONE if complete_ts else f"{CLR_FACTORY_ACTIVE} blink" if running > 0 else CLR_WARN
    return Panel(
        "\n".join(lines),
        title=f"[{border} bold]Factory[/] [{CLR_DIM}]{run_id}[/]",
        border_style=border, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def overnight_panel():
    """Overnight Intelligence — what the swarm did while Paul slept."""
    CC_CACHE = MIRRORDNA / "cc_cache.json"
    SWARM_LOG = MIRRORDNA / "logs" / "swarm_watcher.log"
    GLYPH = MIRRORDNA / "glyphs" / "current.yaml"

    table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    table.add_column("k", width=12, no_wrap=True, style=CLR_DIM)
    table.add_column("v", no_wrap=False, overflow="fold")

    # Cache freshness
    cache_age = "?"
    ships_count = 0
    anti_count = 0
    hot_count = 0
    svc_summary = ""
    repo_dirty = 0
    if CC_CACHE.exists():
        try:
            cache = json.loads(CC_CACHE.read_text())
            compiled = cache.get("compiled_at", "")
            if compiled:
                from datetime import datetime as dt
                try:
                    ct = dt.fromisoformat(compiled)
                    age_min = (dt.now(ct.tzinfo) - ct).total_seconds() / 60
                    if age_min < 60:
                        cache_age = f"{int(age_min)}m ago"
                    else:
                        cache_age = f"{age_min / 60:.1f}h ago"
                except Exception:
                    cache_age = compiled[:16]
            ships_count = len(cache.get("ships", []))
            anti_count = len(cache.get("anti_patterns", []))
            hot_count = len(cache.get("hot_files", []))
            svcs = cache.get("services", {})
            up = sum(1 for v in svcs.values() if v == "up")
            svc_summary = f"{up}/{len(svcs)} up"
            repo_dirty = sum(r.get("uncommitted", 0) for r in cache.get("repos", []))
        except Exception:
            pass

    svc_clr = CLR_OK if svc_summary and svc_summary.split("/")[0] == svc_summary.split("/")[-1].split(" ")[0] else CLR_WARN
    table.add_row("Cache", f"[{BRAND}]{cache_age}[/]")
    if svc_summary:
        table.add_row("Svcs", f"[{svc_clr}]{svc_summary}[/]")
    table.add_row("Ships", f"[{CLR_OK}]{ships_count}[/]")
    if hot_count:
        table.add_row("Hot", f"[{BRAND}]{hot_count}[/] 24h")
    if repo_dirty > 0:
        table.add_row("Dirty", f"[{CLR_WARN if repo_dirty > 10 else CLR_OK}]{repo_dirty}[/]")
    if anti_count:
        table.add_row("Warns", f"[{CLR_ERR}]{anti_count}[/]")

    # Last deep night shift
    if SWARM_LOG.exists():
        try:
            text = SWARM_LOG.read_text()
            # Find last DEEP NIGHT completion
            deep_matches = [l for l in text.splitlines() if "DEEP NIGHT:" in l and "Complete" in l]
            if deep_matches:
                last_deep = deep_matches[-1][:25].strip("[] ")
                table.add_row("Deep shift", f"[{CLR_DIM}]{last_deep}[/]")
            else:
                table.add_row("Deep shift", f"[{CLR_DIM}]Not yet[/]")
        except Exception:
            pass

    # Glyph freshness
    if GLYPH.exists():
        try:
            age_h = (time.time() - GLYPH.stat().st_mtime) / 3600
            clr = CLR_OK if age_h < 24 else CLR_WARN
            table.add_row("Glyph", f"[{clr}]{age_h:.0f}h old[/]")
        except Exception:
            pass

    return Panel(
        table,
        title=f"[{BRAND}]Overnight[/]",
        border_style=BRAND, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def next_panel():
    """What's next — priorities."""
    ctx = get_paul_state()
    table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    table.add_column("m", width=1, no_wrap=True)
    table.add_column("item", no_wrap=False, overflow="fold")

    for i, item in enumerate(ctx["next"][:5]):
        marker = f"[{CLR_NEXT}]▸[/]" if i == 0 else f"[{CLR_DIM}]▸[/]"
        table.add_row(marker, item)

    return Panel(
        table if ctx["next"] else f"  [{CLR_DIM}]Nothing queued[/]",
        title=f"[{CLR_NEXT}]Next[/]",
        border_style=CLR_NEXT, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def loops_panel():
    """Open loops from CONTINUITY.md."""
    ctx = get_paul_state()
    open_items = [(s, i) for s, i in ctx["loops"] if s != "done"]
    done_items = [(s, i) for s, i in ctx["loops"] if s == "done"]

    table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    table.add_column("s", width=2, no_wrap=True)
    table.add_column("item", no_wrap=False, overflow="fold")

    for _, item in open_items:
        table.add_row(f"[{CLR_LOOPS}]○[/]", item)

    if done_items:
        if len(done_items) > 3:
            table.add_row("", f"[{CLR_DIM}]── {len(done_items)} done ──[/]")
        for _, item in done_items[-2:]:
            table.add_row(f"[{CLR_OK}]●[/]", f"[{CLR_DIM}]{item}[/]")

    return Panel(
        table if (open_items or done_items) else f"  [{CLR_DIM}]Clear[/]",
        title=f"[{CLR_LOOPS}]Loops ({len(open_items)})[/]",
        border_style=CLR_LOOPS, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def council_panel():
    """Mirror Council — Real-time deliberation logs."""
    if not COUNCIL_DIR.exists():
        return Panel(f"  [{CLR_DIM}]No active deliberations[/]", title=f"[{CLR_DIM}]Council[/]", border_style=CLR_DIM)
    
    delibs = sorted(COUNCIL_DIR.glob("DELIB-*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not delibs:
        return Panel(f"  [{CLR_DIM}]Council Chamber quiet[/]", title=f"[{CLR_DIM}]Council[/]", border_style=CLR_DIM)
    
    latest_file = delibs[0]
    lines = []
    try:
        content = latest_file.read_text().splitlines()
        # Get the 8 most recent significant lines (excluding delimiters)
        for line in reversed(content):
            if "---" in line or "# " in line or "**Topic**" in line:
                continue
            if line.strip():
                lines.append(f"  {line.strip()[:70]}")
            if len(lines) >= 8:
                break
        lines.reverse()
    except Exception:
        pass
    
    delib_id = latest_file.stem.replace('DELIB-', '')
    title = f"[{CLR_COUNCIL}]Council[/] [grey70]{delib_id}[/]"
    return Panel(
        "\n".join(lines) if lines else f"  [{CLR_DIM}]Listening...[/]",
        title=title,
        border_style=CLR_COUNCIL, box=box.HEAVY_EDGE, padding=(0, 0),
    )



def schedules_panel():
    """Auto-discovered automations — LaunchAgents, git hooks, event scripts, phone daemons."""
    lines = []

    # ── 1. LaunchAgents (dynamic discovery) ──
    la_count = 0
    la_running = 0
    try:
        result = subprocess.run(["launchctl", "list"], capture_output=True, text=True, timeout=5)
        for line in result.stdout.strip().split("\n")[1:]:
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            label = parts[2]
            if not any(k in label.lower() for k in ["mirror", "activemirror", "ollama", "beacon", "pauldesai"]):
                continue
            if any(skip in label for skip in ["com.apple.", "filevault", "siri", "podcast", "VoiceOver", "voicebanking", "voicememo", "displays.Mirror"]):
                continue
            la_count += 1
            pid = parts[0]
            if pid != "-":
                la_running += 1
        dot = f"[{CLR_OK}]●[/]" if la_running > la_count * 0.8 else f"[{CLR_WARN}]●[/]"
        lines.append(f"  {dot} LaunchAgents  [{CLR_DIM}]{la_running}/{la_count} running[/]")
    except Exception:
        lines.append(f"  [{CLR_ERR}]●[/] LaunchAgents  [{CLR_DIM}]error[/]")

    # ── 2. Git hooks (scan repos) ──
    hooks_found = 0
    repos_dir = Path.home() / "repos"
    if repos_dir.exists():
        for repo in repos_dir.iterdir():
            hooks_dir = repo / ".git" / "hooks"
            if hooks_dir.exists():
                for hook in hooks_dir.iterdir():
                    if hook.is_file() and not hook.name.endswith(".sample") and hook.stat().st_mode & 0o111:
                        hooks_found += 1
    dot = f"[{CLR_OK}]●[/]" if hooks_found > 0 else f"[{CLR_DIM}]○[/]"
    lines.append(f"  {dot} Git hooks     [{CLR_DIM}]{hooks_found} active[/]")

    # ── 3. Event scripts (bin/) ──
    bin_dir = MIRRORDNA / "bin"
    bin_count = 0
    if bin_dir.exists():
        bin_count = sum(1 for f in bin_dir.iterdir() if f.is_file() and (f.suffix in (".py", ".sh")))
    dot = f"[{CLR_OK}]●[/]" if bin_count > 0 else f"[{CLR_DIM}]○[/]"
    lines.append(f"  {dot} Event scripts [{CLR_DIM}]{bin_count} in bin/[/]")

    # ── 4. Phone daemons ──
    try:
        result = subprocess.run(["adb", "devices"], capture_output=True, text=True, timeout=3)
        phones = [l for l in result.stdout.strip().split("\n")[1:] if "device" in l and not l.startswith("*")]
        phone_count = len(phones)
        dot = f"[{CLR_OK}]●[/]" if phone_count > 0 else f"[{CLR_DIM}]○[/]"
        lines.append(f"  {dot} Phone daemon  [{CLR_DIM}]{phone_count} device{'s' if phone_count != 1 else ''}[/]")
    except Exception:
        lines.append(f"  [{CLR_DIM}]○[/] Phone daemon  [{CLR_DIM}]no adb[/]")

    # ── 5. GitHub Actions (scan for workflows) ──
    gha_count = 0
    if repos_dir.exists():
        for repo in repos_dir.iterdir():
            wf_dir = repo / ".github" / "workflows"
            if wf_dir.exists():
                gha_count += sum(1 for f in wf_dir.iterdir() if f.suffix in (".yml", ".yaml"))
    if gha_count > 0:
        lines.append(f"  [{CLR_OK}]●[/] GH Actions    [{CLR_DIM}]{gha_count} workflows[/]")

    # ── Summary line ──
    total = la_count + hooks_found + bin_count + gha_count
    lines.append(f"  [{CLR_DIM}]Total: {total} automations[/]")

    return Panel(
        "\n".join(lines),
        title=f"[steel_blue1]Automations[/]",
        border_style="steel_blue1", box=box.HEAVY_EDGE, padding=(0, 0),
    )


def cc_activity_panel():
    """Live Claude Code activity feed — recent tool calls."""
    lines = []
    try:
        if CC_EVENTS.exists():
            # Read last 8 events
            with open(CC_EVENTS) as f:
                all_lines = f.readlines()
            recent = all_lines[-8:] if len(all_lines) >= 8 else all_lines
            for raw in recent:
                try:
                    ev = json.loads(raw)
                    tool = ev.get("tool", "?")
                    target = ev.get("target", "")
                    ts = ev.get("ts", "")
                    # Parse time
                    if ts:
                        try:
                            dt_obj = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            age = (datetime.now(timezone.utc) - dt_obj).total_seconds()
                            if age < 60:
                                age_str = f"{int(age)}s"
                            elif age < 3600:
                                age_str = f"{int(age/60)}m"
                            else:
                                age_str = f"{int(age/3600)}h"
                        except Exception:
                            age_str = ""
                    else:
                        age_str = ""

                    # Color by tool type
                    if tool == "Bash":
                        tc = "yellow"
                    elif tool in ("Read", "Grep", "Glob"):
                        tc = "cyan"
                    elif tool in ("Edit", "Write"):
                        tc = "green"
                    else:
                        tc = CLR_DIM

                    # Shorten target — clean up Bash commands and paths
                    short_target = target.replace(str(Path.home()), "~")
                    # For Bash commands, show first meaningful part
                    if tool == "Bash" and len(short_target) > 25:
                        # Take first command before pipes/semicolons
                        cmd = short_target.split("|")[0].split(";")[0].split("&&")[0].strip()
                        short_target = cmd[:25]
                    elif len(short_target) > 25:
                        short_target = "..." + short_target[-22:]

                    lines.append(f"  [{tc}]{tool:<5}[/] [{CLR_DIM}]{short_target}[/] [{CLR_DIM}]{age_str}[/]")
                except (json.JSONDecodeError, KeyError):
                    pass
    except Exception:
        pass

    lines.reverse()  # newest first
    table = Table(show_header=False, box=None, padding=(0, 0), expand=True)
    table.add_column("content", no_wrap=False, overflow="fold")
    for ln in (lines if lines else [f"  [{CLR_DIM}]No recent activity[/]"]):
        table.add_row(ln)

    return Panel(
        table,
        title=f"[bright_white]CC Activity[/]",
        border_style="bright_white", box=box.HEAVY_EDGE, padding=(0, 0),
    )


def beacon_panel():
    """Beacon stats — post count, last publish."""
    beacon_dir = Path.home() / "repos" / "truth-first-beacon"
    content_dir = beacon_dir / "content" / "reflections"
    lines = []

    # Count posts
    posts = [f for f in content_dir.glob("*.md") if f.name != "_index.md"] if content_dir.exists() else []
    signed = 0
    draft = 0
    newest_name = ""
    newest_time = 0
    for p in posts:
        try:
            text = p.read_text()[:500]
            if "signed: true" in text:
                signed += 1
            if "draft: true" in text:
                draft += 1
            mt = p.stat().st_mtime
            if mt > newest_time:
                newest_time = mt
                newest_name = p.stem
        except Exception:
            pass

    lines.append(f"  Posts: [{CLR_OK}]{len(posts)}[/] | Signed: {signed} | Draft: {draft}")
    if newest_name:
        age_h = (time.time() - newest_time) / 3600
        age_str = f"{int(age_h)}h" if age_h >= 1 else f"{int(age_h*60)}m"
        lines.append(f"  Latest: [{CLR_DIM}]{newest_name[:30]}[/]")
        lines.append(f"  [{CLR_DIM}]{age_str} ago[/]")

    # Next scheduled
    lines.append(f"  Next: [{CLR_DIM}]06:00 / 18:00 auto[/]")

    table = Table(show_header=False, box=None, padding=(0, 0), expand=True)
    table.add_column("content", no_wrap=False, overflow="fold")
    for ln in lines:
        table.add_row(ln)

    return Panel(
        table,
        title=f"[yellow]Beacon[/]",
        border_style="yellow", box=box.HEAVY_EDGE, padding=(0, 0),
    )


def evolution_panel():
    """Evolution Velocity — 7-day self-improvement telemetry."""
    SHIPLOG = MIRRORDNA / "SHIPLOG.md"
    SWARM_HISTORY = MIRRORDNA / "swarm" / "history"
    SELF_SCORES = MIRRORDNA / "state" / "self_scores.jsonl"
    SWARM_LOG = MIRRORDNA / "logs" / "swarm_watcher.log"
    CC_MEMORY = MIRRORDNA / "CC_MEMORY.md"
    RESTART_HISTORY = MIRRORDNA / "health" / "restart_history.json"

    lines = []
    seven_days_ago = time.time() - (7 * 86400)
    total_evolutions = 0

    # ── 1. Ships tracked (SHIPLOG entries with SHIPPED dates in last 7 days) ──
    ships_7d = 0
    if SHIPLOG.exists():
        try:
            today = datetime.now()
            for line in SHIPLOG.read_text().splitlines():
                m = re.search(r'SHIPPED\s+(\d{4}-\d{2}-\d{2})', line)
                if m:
                    try:
                        ship_date = datetime.strptime(m.group(1), "%Y-%m-%d")
                        if (today - ship_date).days <= 7:
                            ships_7d += 1
                    except ValueError:
                        pass
        except Exception:
            pass

    total_evolutions += ships_7d
    if ships_7d > 5:
        sc = CLR_OK
    elif ships_7d >= 2:
        sc = CLR_WARN
    else:
        sc = CLR_ERR
    ship_bar = _bar(ships_7d, max(ships_7d, 12), width=12, fill_color=sc)
    lines.append(f"  Ships [{sc}]{ships_7d}[/] {ship_bar}")

    # ── 2. Factory runs (swarm history files in last 7 days) ──
    factory_runs = 0
    if SWARM_HISTORY.exists():
        try:
            for f in SWARM_HISTORY.iterdir():
                if f.suffix == ".json" and f.stat().st_mtime > seven_days_ago:
                    factory_runs += 1
        except Exception:
            pass

    total_evolutions += factory_runs
    if factory_runs > 5:
        fc = CLR_OK
    elif factory_runs >= 2:
        fc = CLR_WARN
    else:
        fc = CLR_ERR
    factory_bar = _bar(factory_runs, max(factory_runs, 10), width=12, fill_color=fc)
    lines.append(f"  Factory [{fc}]{factory_runs} runs[/] {factory_bar}")

    # ── 3. Auto-heal restarts ──
    heal_count = 0
    if RESTART_HISTORY.exists():
        try:
            rh = json.loads(RESTART_HISTORY.read_text())
            for svc_name, timestamps in rh.items():
                for ts in timestamps:
                    if ts > seven_days_ago:
                        heal_count += 1
        except Exception:
            pass

    hc = CLR_OK if heal_count <= 3 else CLR_WARN if heal_count <= 10 else CLR_ERR
    lines.append(f"  Heals [{hc}]{heal_count} restarts[/]")

    # ── 4. Swarm health (process count + last log age) ──
    swarm_procs = 0
    log_age_str = "?"
    try:
        result = subprocess.run(["pgrep", "-f", "swarm_watcher"], capture_output=True, text=True, timeout=3)
        if result.returncode == 0:
            swarm_procs = len(result.stdout.strip().splitlines())
    except Exception:
        pass

    if SWARM_LOG.exists():
        try:
            age_s = time.time() - SWARM_LOG.stat().st_mtime
            if age_s < 60:
                log_age_str = f"{int(age_s)}s ago"
            elif age_s < 3600:
                log_age_str = f"{int(age_s / 60)}m ago"
            else:
                log_age_str = f"{age_s / 3600:.1f}h ago"
        except Exception:
            pass

    swc = CLR_OK if swarm_procs > 0 else CLR_DIM
    lines.append(f"  Swarm [{swc}]{swarm_procs} procs[/] [{CLR_DIM}]log {log_age_str}[/]")

    # ── 5. Context cache age (CC_MEMORY.md) ──
    if CC_MEMORY.exists():
        try:
            age_m = (time.time() - CC_MEMORY.stat().st_mtime) / 60
            if age_m < 60:
                cache_str = f"{int(age_m)}m old"
                cc = CLR_OK
            elif age_m < 360:
                cache_str = f"{age_m / 60:.1f}h old"
                cc = CLR_WARN
            else:
                cache_str = f"{age_m / 60:.0f}h old"
                cc = CLR_ERR
            lines.append(f"  Cache [{cc}]CC_MEMORY {cache_str}[/]")
        except Exception:
            pass
    else:
        lines.append(f"  Cache [{CLR_DIM}]CC_MEMORY missing[/]")

    # ── 6. Self-score trend ──
    if SELF_SCORES.exists():
        try:
            scores = []
            with open(SELF_SCORES) as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        scores.append(entry.get("score", 0))
                    except (json.JSONDecodeError, KeyError):
                        pass
            if scores:
                recent = scores[-3:] if len(scores) >= 3 else scores
                trend_str = " -> ".join(str(s) for s in recent)
                if len(recent) >= 2:
                    if recent[-1] > recent[0]:
                        trend_tag = "up"
                        tc = CLR_OK
                    elif recent[-1] < recent[0]:
                        trend_tag = "down"
                        tc = CLR_ERR
                    else:
                        trend_tag = "flat"
                        tc = CLR_WARN
                else:
                    trend_tag = ""
                    tc = CLR_DIM
                lines.append(f"  Score [{tc}]{trend_str}[/] [{CLR_DIM}]({trend_tag})[/]")
        except Exception:
            pass

    # ── Overall velocity color ──
    if total_evolutions > 5:
        border = CLR_OK
        vel_tag = f"[{CLR_OK} bold]HIGH[/]"
    elif total_evolutions >= 2:
        border = CLR_WARN
        vel_tag = f"[{CLR_WARN} bold]MODERATE[/]"
    else:
        border = CLR_ERR
        vel_tag = f"[{CLR_ERR} bold]LOW[/]"

    lines.insert(0, f"  {vel_tag} [{CLR_DIM}]{total_evolutions} evolutions[/]")

    return Panel(
        "\n".join(lines),
        title=f"[{CLR_EVO}]Evolution Velocity (7d)[/]",
        border_style=border, box=box.HEAVY_EDGE, padding=(0, 0),
    )


CLR_TRANSPARENCY = "deep_sky_blue1"
HOOK_DECISIONS = BUS / "hook_decisions.jsonl"
SELF_CRITIQUE = MIRRORDNA / "self_critique.jsonl"


def agent_transparency_panel():
    """Agent Transparency — hook decisions, gate status, self-critique, failure points.

    Makes Claude's black box visible. Shows what hooks fired, what was
    blocked/allowed/warned, recurring mistakes, and active gates.
    """
    lines = []

    # ── 1. Hook decision summary (last 24h) ──
    deny_count = 0
    warn_count = 0
    allow_count = 0
    block_count = 0

    recent_decisions = []
    if HOOK_DECISIONS.exists():
        try:
            cutoff = time.time() - 86400
            with open(HOOK_DECISIONS) as f:
                all_lines = f.readlines()
            for raw in all_lines:
                try:
                    ev = json.loads(raw.strip())
                    if ev.get("epoch", 0) >= cutoff:
                        d = ev.get("decision", "allow")
                        if d == "deny":
                            deny_count += 1
                        elif d == "warn":
                            warn_count += 1
                        elif d == "block":
                            block_count += 1
                        else:
                            allow_count += 1
                except json.JSONDecodeError:
                    pass
            # Last 6 decisions for detail view
            for raw in all_lines[-6:]:
                try:
                    recent_decisions.append(json.loads(raw.strip()))
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass

    # Summary bar
    total = deny_count + warn_count + block_count + allow_count
    if total > 0:
        if deny_count + block_count > 0:
            summary_clr = CLR_ERR
            summary_tag = f"[{CLR_ERR} bold]{deny_count + block_count} BLOCKED[/]"
        elif warn_count > 0:
            summary_clr = CLR_WARN
            summary_tag = f"[{CLR_WARN} bold]{warn_count} WARNED[/]"
        else:
            summary_clr = CLR_OK
            summary_tag = f"[{CLR_OK} bold]CLEAN[/]"
        lines.append(
            f"  {summary_tag} [{CLR_DIM}]{total} decisions "
            f"({allow_count} ok, {warn_count} warn, {deny_count + block_count} deny)[/]"
        )
    else:
        lines.append(f"  [{CLR_DIM}]No hook decisions logged yet[/]")

    # ── 2. Recent hook decisions (last 6) ──
    if recent_decisions:
        lines.append("")
        for ev in reversed(recent_decisions):
            hook = ev.get("hook", "?")[:16]
            decision = ev.get("decision", "?")
            reason = ev.get("reason", "")[:35]
            ts = ev.get("ts", "")

            # Age string
            age_str = ""
            if ev.get("epoch"):
                age_s = time.time() - ev["epoch"]
                if age_s < 60:
                    age_str = f"{int(age_s)}s"
                elif age_s < 3600:
                    age_str = f"{int(age_s / 60)}m"
                else:
                    age_str = f"{int(age_s / 3600)}h"

            # Color by decision
            if decision in ("deny", "block"):
                dc = CLR_ERR
                icon = "X"
            elif decision == "warn":
                dc = CLR_WARN
                icon = "!"
            else:
                dc = CLR_OK
                icon = "."

            lines.append(
                f"  [{dc}]{icon}[/] [{CLR_DIM}]{hook:<14}[/] [{dc}]{decision:<5}[/] "
                f"[{CLR_DIM}]{reason}[/] [{CLR_DIM}]{age_str}[/]"
            )

    # ── 3. Self-critique score (latest) ──
    critique_score = None
    recurring = []
    if SELF_CRITIQUE.exists():
        try:
            last_line = ""
            with open(SELF_CRITIQUE) as f:
                for line in f:
                    if line.strip():
                        last_line = line
            if last_line:
                entry = json.loads(last_line)
                critique_score = entry.get("score")
                recurring = entry.get("recurring", [])
        except Exception:
            pass

    if critique_score is not None:
        if critique_score >= 7:
            sc = CLR_OK
        elif critique_score >= 4:
            sc = CLR_WARN
        else:
            sc = CLR_ERR
        score_bar = _bar(critique_score, 10, width=10, fill_color=sc)
        lines.append(f"\n  Score [{sc}]{critique_score}/10[/] {score_bar}")
        if recurring:
            for r in recurring[:2]:
                lines.append(f"  [{CLR_ERR}]! {r[:50]}[/]")

    # ── 4. Active gates ──
    gates = {
        "fact_check": Path(HOME / ".activemirror/bin/fact_check_hook.py").exists(),
        "deploy_gate": Path(HOME / ".activemirror/bin/deploy_gate.py").exists(),
        "anti_rational": Path(HOME / ".activemirror/bin/anti_rationalization_gate.py").exists(),
        "rabbit_hole": Path(HOME / ".activemirror/bin/rabbit_hole_detector.py").exists(),
        "duplicates": Path(HOME / ".activemirror/bin/duplicate_detector.py").exists(),
    }
    active = sum(1 for v in gates.values() if v)
    gate_str = " ".join(
        f"[{CLR_OK}]{k[:8]}[/]" if v else f"[{CLR_ERR}]{k[:8]}[/]"
        for k, v in gates.items()
    )
    lines.append(f"\n  Gates [{CLR_OK}]{active}/{len(gates)}[/] {gate_str}")

    border = CLR_ERR if (deny_count + block_count) > 0 else CLR_WARN if warn_count > 0 else CLR_TRANSPARENCY

    return Panel(
        "\n".join(lines),
        title=f"[{CLR_TRANSPARENCY}]Agent Transparency[/]",
        border_style=border, box=box.HEAVY_EDGE, padding=(0, 0),
    )


def _load_service_registry():
    """Load service registry — single source of truth."""
    reg_path = MIRRORDNA / "SERVICE_REGISTRY.json"
    try:
        return json.loads(reg_path.read_text())
    except Exception:
        return {"services": [], "scheduled_tasks": []}


def _check_port(port, listening_output):
    """Check if a port is in lsof output."""
    return f":{port} " in listening_output or f":{port}\n" in listening_output


def _notify_service_down(name, detail=""):
    """Send macOS notification for a service going down. One-shot per service."""
    alert_key = f"{name}_down"
    if _service_alerts.get(alert_key):
        return  # Already alerted
    _service_alerts[alert_key] = True
    msg = f"{name} is DOWN"
    if detail:
        msg += f" — {detail}"
    try:
        subprocess.Popen([
            "osascript", "-e",
            f'display notification "{msg}" with title "⟡ MirrorDNA" subtitle "Service Alert"'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _notify_service_up(name):
    """Clear alert state when service recovers."""
    alert_key = f"{name}_down"
    if _service_alerts.get(alert_key):
        _service_alerts[alert_key] = False
        try:
            subprocess.Popen([
                "osascript", "-e",
                f'display notification "{name} recovered" with title "⟡ MirrorDNA" subtitle "Service OK"'
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass


def fleet_panel():
    """Full fleet — devices, services, mesh, models. Registry-driven."""
    lines = []
    registry = _load_service_registry()

    # ── Devices (ADB + Tailscale fallback) ──
    KNOWN_DEVICES = {
        "komodo": "Pixel 9 Pro XL",
        "CPH2745": "OnePlus 15",
        "OP611FL1": "OnePlus 15",
    }
    # Tailscale DNS patterns → display names (fallback when ADB offline)
    TS_DEVICE_MAP = {
        "google-pixel-9-pro-xl": "Pixel 9 Pro XL",
        "oneplus-cph2745": "OnePlus 15",
        "mirrorbrain-red": "Mac mini (Red)",
        "paul-macbook-air": "MacBook Air",
    }
    found = set()
    # First: check ADB (USB + wireless)
    try:
        result = subprocess.run(["adb", "devices", "-l"], capture_output=True, text=True, timeout=3)
        for line in result.stdout.strip().split("\n")[1:]:
            if "device" not in line or line.startswith("*"):
                continue
            for key, name in KNOWN_DEVICES.items():
                if key in line and name not in found:
                    found.add(name)
                    conn = "USB" if "usb:" in line else "net"
                    lines.append(f"  [{CLR_OK}]●[/] {name} [{CLR_DIM}]{conn}[/]")
    except Exception:
        pass
    # Second: Tailscale fallback for anything not found via ADB
    ts_data = {}
    try:
        result = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=3)
        ts_data = json.loads(result.stdout)
        for peer in ts_data.get("Peer", {}).values():
            dns = peer.get("DNSName", "").split(".")[0]
            hostname = peer.get("HostName", "")
            # Match by DNS name prefix or hostname
            name = None
            for pattern, display in TS_DEVICE_MAP.items():
                if pattern in dns or pattern in hostname.lower().replace("'s ", "-").replace(" ", "-"):
                    name = display
                    break
            if name and name not in found:
                found.add(name)
                if peer.get("Online"):
                    lines.append(f"  [{CLR_OK}]●[/] {name} [{CLR_DIM}]mesh[/]")
                else:
                    lines.append(f"  [{CLR_ERR}]●[/] {name} [{CLR_DIM}]off[/]")
    except Exception:
        pass
    # Anything still missing
    all_known = set(KNOWN_DEVICES.values()) | set(TS_DEVICE_MAP.values())
    for missing in all_known - found:
        lines.append(f"  [{CLR_ERR}]●[/] {missing} [{CLR_DIM}]off[/]")

    # ── Tailscale mesh summary ──
    try:
        if not ts_data:
            result = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=3)
            ts_data = json.loads(result.stdout)
        peers = ts_data.get("Peer", {})
        mesh_on = sum(1 for p in peers.values() if p.get("Online"))
        total = len(peers)
        lines.append(f"  [{CLR_OK}]●[/] Mesh [{CLR_DIM}]{mesh_on}/{total} peers[/]")
    except Exception:
        lines.append(f"  [{CLR_DIM}]○ Mesh unavail[/]")

    # ── Services from registry (port-based) ──
    try:
        result = subprocess.run(
            ["lsof", "-iTCP", "-sTCP:LISTEN", "-P", "-n"],
            capture_output=True, text=True, timeout=5)
        listening = result.stdout
    except Exception:
        listening = ""

    svc_up = 0
    svc_total = 0
    critical_down = []
    dots = ""

    for svc in registry.get("services", []):
        port = svc.get("port")
        if not port:
            continue  # Process-based checks handled separately
        svc_total += 1
        is_up = _check_port(port, listening)
        name = svc["name"]

        if is_up:
            dots += f"[{CLR_OK}]●[/]"
            svc_up += 1
            _notify_service_up(name)
        else:
            dots += f"[{CLR_ERR}]●[/]"
            if svc.get("critical"):
                critical_down.append(name)
                deps = svc.get("depends_on", [])
                detail = f"deps: {', '.join(deps)}" if deps else ""
                _notify_service_down(name, detail)

    lines.append(f"  {dots} {svc_up}/{svc_total}")
    if critical_down:
        lines.append(f"  [{CLR_ERR} bold]DOWN: {', '.join(critical_down[:3])}[/]")

    # ── Process-based services ──
    for svc in registry.get("services", []):
        proc = svc.get("process")
        if not proc or svc.get("port"):
            continue
        try:
            result = subprocess.run(["pgrep", "-x", proc], capture_output=True, timeout=2)
            if result.returncode == 0:
                _notify_service_up(svc["name"])
            else:
                if svc.get("critical"):
                    critical_down.append(svc["name"])
                    _notify_service_down(svc["name"])
        except Exception:
            pass

    # ── Ollama loaded model ──
    try:
        resp = urllib.request.urlopen("http://localhost:11434/api/ps", timeout=2)
        data = json.loads(resp.read())
        models = data.get("models", [])
        if models:
            m = models[0]
            mname = m.get("name", "?")[:16]
            vram = m.get("size_vram", 0) // 1024 // 1024
            lines.append(f"  [{CLR_OK}]●[/] {mname} {vram}M")
        else:
            lines.append(f"  [{CLR_WARN}]●[/] Ollama [{CLR_DIM}]idle[/]")
    except Exception:
        pass  # Already shown as DOWN in service grid

    # ── Scheduled task health ──
    for task in registry.get("scheduled_tasks", []):
        log_path = task.get("log", "")
        if not log_path:
            continue
        log_path = Path(log_path.replace("~", str(Path.home())))
        if task.get("critical") and log_path.exists():
            age_h = (time.time() - log_path.stat().st_mtime) / 3600
            if age_h > 13:  # Should fire every 12h max
                lines.append(f"  [{CLR_ERR}]●[/] {task['name']} [{CLR_ERR}]stale {int(age_h)}h[/]")
                _notify_service_down(task["name"], f"log stale {int(age_h)}h")

    return Panel(
        "\n".join(lines),
        title=f"[orchid1]Fleet[/]",
        border_style="orchid1", box=box.HEAVY_EDGE, padding=(0, 0),
    )


# ─── Layout ──────────────────────────────────────────────────────────────────

def build_dashboard():
    """Build the full dashboard — branded, 2-column asymmetric."""
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=1),
    )

    layout["body"].split_row(
        Layout(name="left", ratio=3),
        Layout(name="right", ratio=5),
    )

    layout["left"].split_column(
        Layout(name="infra", ratio=2),
        Layout(name="fleet", ratio=3, minimum_size=8),
        Layout(name="vitals", ratio=2),
        Layout(name="schedules", ratio=2),
    )

    factory_active = _find_active_factory_run() is not None
    layout["right"].split_column(
        Layout(name="top_row", ratio=2),
        Layout(name="mid_row", ratio=2),
        Layout(name="agent_row", ratio=2),
        Layout(name="factory_row", ratio=3 if factory_active else 1, minimum_size=3),
        Layout(name="bottom_row", ratio=2),
    )

    layout["top_row"].split_row(
        Layout(name="sessions", ratio=3),
        Layout(name="paul", ratio=2),
    )

    layout["mid_row"].split_row(
        Layout(name="cc_activity", ratio=3),
        Layout(name="evolution", ratio=2),
        Layout(name="beacon", ratio=2),
    )

    layout["bottom_row"].split_row(
        Layout(name="next", ratio=1),
        Layout(name="loops", ratio=1),
        Layout(name="overnight", ratio=1),
    )

    # Populate
    layout["header"].update(header_panel())
    layout["infra"].update(infra_panel())
    layout["fleet"].update(fleet_panel())
    layout["vitals"].update(vitals_panel())
    layout["schedules"].update(schedules_panel())
    layout["sessions"].update(session_panel())
    layout["paul"].update(paul_panel())
    layout["cc_activity"].update(cc_activity_panel())
    layout["evolution"].update(evolution_panel())
    layout["beacon"].update(beacon_panel())
    layout["agent_row"].update(agent_transparency_panel())
    layout["factory_row"].update(factory_panel())
    layout["next"].update(next_panel())
    layout["loops"].update(loops_panel())
    layout["overnight"].update(overnight_panel())

    footer = Text.from_markup(
        f" [{CLR_DIM}]q=quit[/]  [{CLR_DIM}]30s refresh[/]  "
        f"[{CLR_DIM}]~/.mirrordna/[/]  [{BRAND}]MirrorDNA[/]"
    )
    layout["footer"].update(footer)

    return layout


def main():
    """Run dashboard with live refresh."""
    if "--once" in sys.argv:
        console.print(build_dashboard())
        return

    try:
        with Live(build_dashboard(), console=console, refresh_per_second=1, screen=True) as live:
            while True:
                time.sleep(5)
                live.update(build_dashboard())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
