#!/usr/bin/env python3
"""
MirrorDNA System Map — Live terminal dashboard.
Visual topology of all services, automations, and health.

Usage:
    python3 system_map.py          # live dashboard (refreshes every 5s)
    python3 system_map.py --once   # single snapshot
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.live import Live
    from rich.align import Align
    from rich import box
except ImportError:
    print("pip install rich")
    sys.exit(1)

HOME = Path.home()
MIRRORDNA = HOME / ".mirrordna"

console = Console()

# ── Glyphs ──
G = {
    "ollama": "⬡", "brain_api": "◈", "mirrorgate": "⊞", "hub": "⊕",
    "router": "⊞", "mirrorbalance": "⚖", "factory": "⚙", "beacon": "◉",
    "beacon_chat": "◉", "cloudflared": "☁", "claude_proxy": "◇",
    "heartbeat": "♡", "kavach": "⛨", "chetana": "⟡",
    "up": "●", "down": "✗", "stale": "◌", "pass": "✓", "fail": "✗",
}

# ── Service checks — loaded from SERVICE_REGISTRY.json (single source of truth) ──
def _load_services():
    """Build SERVICES dict from registry."""
    reg_path = MIRRORDNA / "SERVICE_REGISTRY.json"
    glyphs = {
        "Ollama": "⬡", "Brain API": "◈", "MirrorGate": "⊞", "Sovereign Hub": "⊕",
        "Inference Router": "⊞", "Safety Proxy": "⛊", "Factory Trigger": "⚙",
        "MirrorHint": "◈", "Kickoff Guard": "⊘", "Confidence Cockpit": "◈",
        "ActiveMirror UI": "⊞", "Web Dashboard": "◈", "Claude Proxy": "◇",
        "Chat Server": "◉", "MCP SSE": "⊞", "Mirror Radio": "◉",
        "Ticker Server": "◈", "Trust Engine": "⛨", "Mesh Relay": "⚖",
        "MirrorRadar": "◉", "Swarm Coordinator": "⟡", "Kavach Server": "⛨",
        "Voice Orchestrator": "◎",
    }
    tier_map = {
        "inference": "local", "core": "core", "factory": "agent",
        "ui": "public", "content": "public", "intelligence": "agent",
        "network": "mesh", "voice": "agent", "security": "public",
    }
    # Known health paths for services without health_url in registry
    known_paths = {
        "Trust Engine": "/",
        "Chat Server": "/api/chat/health",
        "MirrorGate": "/api/health",
        "Mesh Relay": "/ws",
        "Mirror Radio": "/",
    }
    ws_services = {"Mesh Relay", "Voice Orchestrator", "Redis", "MirrorHint", "ActiveMirror UI", "Mirror Radio"}
    try:
        data = json.loads(reg_path.read_text())
    except Exception:
        return {}
    services = {}
    for svc in data.get("services", []):
        port = svc.get("port")
        if not port:
            continue
        name = svc["name"]
        key = name.lower().replace(" ", "_")
        health = svc.get("health_url", "")
        if health and "localhost" in health:
            path = "/" + health.split("/", 3)[-1].lstrip("/")
        elif name in known_paths:
            path = known_paths[name]
        else:
            path = "/health"
        cat = svc.get("category", "core")
        services[key] = {
            "port": port,
            "path": path,
            "glyph": glyphs.get(name, "●"),
            "tier": tier_map.get(cat, "core"),
            "name": name,
            "ws": name in ws_services,
        }
    return services

SERVICES = _load_services()

AUTOMATIONS = [
    {"name": "Heartbeat",        "label": "ai.mirrordna.continuity-heartbeat", "interval": "60s",    "glyph": "♡"},
    {"name": "Smoke Test",       "label": "ai.mirrordna.smoke-test",           "interval": "8:00 AM", "glyph": "✓"},
    {"name": "Overnight Report", "label": "com.mirrordna.overnight-report",    "interval": "3:00 AM", "glyph": "📋"},
    {"name": "Overnight Coder",  "label": "ai.mirrordna.overnight-coder",      "interval": "0:30 AM", "glyph": "⬡"},
    {"name": "Site Autopublish", "label": "com.mirrordna.beacon-autoupdate",   "interval": "7/23:00", "glyph": "◉"},
    {"name": "Context Compiler", "label": "ai.mirrordna.context-compiler",     "interval": "2h",      "glyph": "◈"},
]


def http_check(port: int, path: str, timeout: float = 2.0, ws: bool = False) -> tuple:
    """Check if service is up. Returns (is_up, latency_ms)."""
    if ws:
        # For WebSocket services, just check if port is listening
        import socket
        try:
            start = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect(("localhost", port))
            s.close()
            ms = round((time.time() - start) * 1000)
            return True, ms
        except Exception:
            return False, 0
    url = f"http://localhost:{port}{path}"
    try:
        start = time.time()
        req = urllib.request.Request(url, headers={"User-Agent": "MirrorMap/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
            ms = round((time.time() - start) * 1000)
            return True, ms
    except Exception:
        return False, 0


def check_launchagent(label: str) -> str:
    """Check if a LaunchAgent is loaded."""
    try:
        result = subprocess.run(
            ["launchctl", "list"], capture_output=True, text=True, timeout=3
        )
        for line in result.stdout.splitlines():
            if label in line:
                parts = line.split()
                pid = parts[0] if parts[0] != "-" else None
                return "running" if pid else "loaded"
        return "not loaded"
    except Exception:
        return "unknown"


def get_smoke_results() -> dict:
    """Read latest smoke test results."""
    path = MIRRORDNA / "bus" / "continuity" / "smoke_results.json"
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text())
        return data
    except Exception:
        return {}


def get_overnight_status() -> str:
    """Check overnight coder status."""
    results_dir = MIRRORDNA / "overnight" / "results"
    queue_file = MIRRORDNA / "overnight" / "queue.yaml"
    if not results_dir.exists():
        return "no results yet"
    dates = sorted(results_dir.iterdir(), reverse=True)
    if dates:
        latest = dates[0]
        summary = latest / "summary.json"
        if summary.exists():
            data = json.loads(summary.read_text())
            completed = sum(1 for r in data if r.get("status") == "completed")
            return f"{latest.name}: {completed}/{len(data)} done"
    # Check queue
    if queue_file.exists():
        text = queue_file.read_text()
        tasks = text.count("- title:")
        if tasks > 0:
            return f"{tasks} tasks queued"
    return "idle"


def get_portless_routes() -> list:
    """Read portless routes.json and check each."""
    routes_file = Path.home() / ".portless" / "routes.json"
    try:
        if not routes_file.exists():
            return []
        data = json.loads(routes_file.read_text())
        results = []
        for r in data:
            hostname = r.get("hostname", "?")
            port = r.get("port", 0)
            name = hostname.replace(".localhost", "")
            up, latency = http_check(port, "/health") if port else (False, 0)
            if not up:
                up, latency = http_check(port, "/") if port else (False, 0)
            results.append({"name": name, "hostname": hostname, "port": port, "up": up, "latency": latency})
        return results
    except Exception:
        return []


def get_disk_info() -> str:
    """Get disk usage."""
    try:
        result = subprocess.run(
            ["df", "-h", "/"], capture_output=True, text=True, timeout=3
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            return f"{parts[4]} used ({parts[3]} free)"
    except Exception:
        pass
    return "unknown"


def get_continuity_age() -> str:
    """How fresh is CONTINUITY.md."""
    path = MIRRORDNA / "CONTINUITY.md"
    try:
        mtime = path.stat().st_mtime
        age = time.time() - mtime
        if age < 300:
            return "live"
        elif age < 3600:
            return f"{int(age/60)}m ago"
        elif age < 86400:
            return f"{int(age/3600)}h ago"
        else:
            return f"{int(age/86400)}d ago"
    except Exception:
        return "missing"


def build_dashboard() -> Layout:
    """Build the full dashboard layout."""
    now = datetime.now()
    layout = Layout()

    # ── Header ──
    header_text = Text()
    header_text.append("◇ ", style="bold yellow")
    header_text.append("MirrorDNA System Map", style="bold white")
    header_text.append(f"  ·  {now.strftime('%Y-%m-%d %H:%M:%S')} IST", style="white")
    header_text.append(f"  ·  Continuity: {get_continuity_age()}", style="cyan")
    header = Panel(Align.center(header_text), box=box.HEAVY, style="bright_cyan", height=3)

    # ── Services ──
    svc_table = Table(box=box.SIMPLE_HEAVY, show_edge=False, pad_edge=False, expand=True)
    svc_table.add_column("", width=3, justify="center")
    svc_table.add_column("Service", min_width=14)
    svc_table.add_column("", width=3, justify="center")
    svc_table.add_column("Port", width=6, justify="right")
    svc_table.add_column("Latency", width=8, justify="right")
    svc_table.add_column("Tier", width=7)

    up_count = 0
    total = len(SERVICES)
    for key, info in SERVICES.items():
        is_up, latency = http_check(info["port"], info["path"], ws=info.get("ws", False))
        if is_up:
            up_count += 1
        status_icon = "[green]●[/]" if is_up else "[red]✗[/]"
        glyph = info["glyph"]
        display_name = info.get("name", key)
        latency_str = f"{latency}ms" if is_up else "—"
        lat_style = "green" if latency < 100 else "yellow" if latency < 500 else "red"
        tier_colors = {"local": "cyan", "core": "magenta", "mesh": "blue", "agent": "yellow", "public": "green", "proxy": "bright_cyan"}
        tier_style = tier_colors.get(info["tier"], "white")

        svc_table.add_row(
            f"{glyph}",
            f"[bold]{display_name}[/]",
            status_icon,
            f":{info['port']}",
            f"[{lat_style}]{latency_str}[/]" if is_up else "—",
            f"[{tier_style}]{info['tier']}[/]",
        )

    # Add portless routes to the service table
    routes = get_portless_routes()
    route_up = 0
    if routes:
        svc_table.add_row("", "[bright_cyan]── portless routes ──[/]", "", "", "", "", end_section=True)
        for r in routes:
            is_r_up = r["up"]
            if is_r_up:
                route_up += 1
            status_icon = "[green]●[/]" if is_r_up else "[red]✗[/]"
            lat_str = f"{r['latency']}ms" if is_r_up else "—"
            lat_style = "green" if r["latency"] < 100 else "yellow"
            svc_table.add_row(
                "⊘",
                f"{r['name']}",
                status_icon,
                f":{r['port']}",
                f"[{lat_style}]{lat_str}[/]" if is_r_up else "—",
                f"[bright_cyan]portless[/]",
            )

    svc_header = f"Services  [green]{up_count}[/]/[white]{total}[/] up  ·  Portless [green]{route_up}[/]/[white]{len(routes)}[/] routed"
    down_count = total - up_count
    if down_count == 0:
        svc_border = "green"
    elif down_count <= 3:
        svc_border = "yellow"
    else:
        svc_border = "red blink"
    services_panel = Panel(svc_table, title=svc_header, border_style=svc_border, box=box.HEAVY_EDGE)

    # ── Topology Map — ports from SERVICES (registry) ──
    def _svc_check(key):
        """Check a service by registry key."""
        s = SERVICES.get(key, {})
        if not s:
            return False, 0
        return http_check(s["port"], s["path"], ws=s.get("ws", False))

    topo = Text()
    topo.append("                    ☁ cloudflared\n", style="cyan")
    topo.append("                    │\n", style="white")
    topo.append("           ┌────────┼────────┐\n", style="white")
    topo.append("           │        │        │\n", style="white")
    topo.append("       ", style="")

    b_up, _ = _svc_check("trust_engine")
    topo.append("◉ beacon", style="green" if b_up else "red")
    topo.append("   ", style="")
    c_up, _ = _svc_check("chat_server")
    topo.append("◉ chat", style="green" if c_up else "red")
    topo.append("    ", style="")
    ch_up, _ = _svc_check("swarm_coordinator")
    topo.append("⟡ swarm\n", style="green" if ch_up else "red")

    topo.append("           │        │\n", style="white")
    topo.append("           └────────┤\n", style="white")
    topo.append("                    │\n", style="white")

    g_up, _ = _svc_check("mirrorgate")
    topo.append("              ", style="")
    topo.append("⊞ mirrorgate", style="green" if g_up else "red")
    topo.append("\n", style="")
    topo.append("           ┌────────┼────────┐\n", style="white")

    h_up, _ = _svc_check("sovereign_hub")
    o_up, _ = _svc_check("ollama")
    p_up, _ = _svc_check("claude_proxy")
    topo.append("       ", style="")
    topo.append("⊕ hub", style="green" if h_up else "red")
    topo.append("      ", style="")
    topo.append("⬡ ollama", style="green" if o_up else "red")
    topo.append("    ", style="")
    topo.append("◇ proxy", style="green" if p_up else "red")
    topo.append("\n", style="")

    topo.append("           │        │        │\n", style="white")
    f_up, _ = _svc_check("factory_trigger")
    br_up, _ = _svc_check("brain_api")
    m_up, _ = _svc_check("mesh_relay")
    topo.append("       ", style="")
    topo.append("⚙ factory", style="green" if f_up else "red")
    topo.append("   ", style="")
    topo.append("◈ brain", style="green" if br_up else "red")
    topo.append("     ", style="")
    topo.append("⚖ mesh", style="green" if m_up else "red")
    topo.append("\n", style="")

    topology_panel = Panel(topo, title="Topology", border_style="cyan", box=box.HEAVY_EDGE)

    # ── Automations ──
    auto_table = Table(box=box.SIMPLE, show_edge=False, expand=True)
    auto_table.add_column("", width=2)
    auto_table.add_column("Automation", min_width=16)
    auto_table.add_column("Schedule", width=10)
    auto_table.add_column("Status", width=12)

    for auto in AUTOMATIONS:
        status = check_launchagent(auto["label"])
        status_style = "green" if status in ("running", "loaded") else "red"
        auto_table.add_row(
            auto["glyph"],
            f"[bold]{auto['name']}[/]",
            f"{auto['interval']}",
            f"[{status_style}]{status}[/]",
        )

    automations_panel = Panel(auto_table, title="Automations", border_style="yellow", box=box.HEAVY_EDGE)

    # ── Health Summary ──
    smoke = get_smoke_results()
    health_lines = Text()

    if smoke:
        ts = smoke.get("timestamp", "")
        failures = [r for r in smoke.get("results", []) if r.get("status") != "pass"]
        passed = sum(1 for r in smoke.get("results", []) if r.get("status") == "pass")
        total_tests = len(smoke.get("results", []))

        if failures:
            health_lines.append(f"Smoke: {passed}/{total_tests} passed", style="yellow")
            health_lines.append("\n")
            for f in failures[:5]:
                health_lines.append(f"  ✗ {f.get('name', '?')}: {f.get('detail', '')[:40]}\n", style="red")
        else:
            health_lines.append(f"Smoke: {total_tests}/{total_tests} passed ✓\n", style="green")

        # Age of smoke test
        try:
            smoke_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            age_h = (datetime.now(smoke_time.tzinfo) - smoke_time).total_seconds() / 3600
            age_str = f"{int(age_h)}h ago" if age_h > 1 else "recent"
            health_lines.append(f"  Last run: {age_str}\n", style="white")
        except Exception:
            pass
    else:
        health_lines.append("Smoke: no results\n", style="white")

    health_lines.append(f"\nDisk: {get_disk_info()}\n", style="white")

    overnight = get_overnight_status()
    health_lines.append(f"Overnight: {overnight}\n", style="cyan")

    # Provider chain
    health_lines.append("\nChat providers: ", style="white")
    try:
        chat_port = SERVICES.get("chat_server", {}).get("port", 8095)
        resp = http_check(chat_port, "/api/chat/health")
        if resp[0]:
            raw = urllib.request.urlopen(
                urllib.request.Request(f"http://localhost:{chat_port}/api/chat/health",
                                     headers={"User-Agent": "MirrorMap/1.0"}),
                timeout=2
            ).read().decode()
            data = json.loads(raw)
            providers = data.get("providers", [])
            primary = data.get("primary", "none")
            health_lines.append(f"{primary}", style="bold green")
            if len(providers) > 1:
                health_lines.append(f" (+{len(providers)-1} fallback)", style="white")
        else:
            health_lines.append("offline", style="red")
    except Exception:
        health_lines.append("unknown", style="white")
    health_lines.append("\n", style="")

    health_panel = Panel(health_lines, title="Health & Monitoring", border_style="magenta", box=box.HEAVY_EDGE)

    # ── Pipeline view ──
    pipe = Text()
    pipe.append("Daily Pipeline\n\n", style="bold")
    steps = [
        ("00:30", "⬡ Overnight Coder", "Ollama runs queued code tasks"),
        ("03:00", "📋 Overnight Report", "Generate + email daily report"),
        ("07:00", "◉ Site Autopublish", "Rebuild beacon + reflections"),
        ("08:00", "✓ Smoke Test", "16 e2e checks, alert on failure"),
        ("  60s", "♡ Heartbeat", "Update Now.md, check services"),
        ("  2hr", "◈ Context Compiler", "Rebuild CC_MEMORY.md cache"),
    ]
    for t, icon, desc in steps:
        pipe.append(f"  {t}  ", style="white")
        pipe.append(f"{icon}  ", style="yellow")
        pipe.append(f"{desc}\n", style="white")

    pipe.append("\nIncoming Chain\n\n", style="bold")
    chain = [
        ("☁", "cloudflared", "→", "◉", "beacon / chat"),
        ("⊞", "mirrorgate", "→", "⊕", "hub → services"),
        ("◇", "claude proxy", "→", "⬡", "ollama (fallback)"),
        ("⚙", "factory", "→", "◇", "claude code agents"),
    ]
    for g1, n1, arrow, g2, n2 in chain:
        pipe.append(f"  {g1} {n1:14s} ", style="cyan")
        pipe.append(f"{arrow} ", style="white")
        pipe.append(f"{g2} {n2}\n", style="")

    pipeline_panel = Panel(pipe, title="Pipeline", border_style="blue", box=box.HEAVY_EDGE)

    # ── Assemble layout ──
    layout.split_column(
        Layout(header, size=3),
        Layout(name="top", ratio=4),
        Layout(name="bottom", ratio=2),
    )

    layout["top"].split_row(
        Layout(services_panel, name="services", ratio=1),
        Layout(topology_panel, name="topology", ratio=1),
    )

    layout["bottom"].split_row(
        Layout(automations_panel, ratio=1),
        Layout(health_panel, ratio=1),
        Layout(pipeline_panel, ratio=1),
    )

    return layout


def main():
    once = "--once" in sys.argv

    if once:
        layout = build_dashboard()
        console.print(layout)
        return

    console.clear()
    try:
        with Live(build_dashboard(), console=console, refresh_per_second=0.2, screen=True) as live:
            while True:
                time.sleep(5)
                live.update(build_dashboard())
    except KeyboardInterrupt:
        console.print("\n[dim]System map stopped.[/]")


if __name__ == "__main__":
    main()
