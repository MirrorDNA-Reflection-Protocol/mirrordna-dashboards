# MirrorDNA Dashboards

Cognitive terminal dashboards for monitoring AI systems, services, and operational intelligence.

## Features

### Cognitive Dashboard
Live terminal UI showing:
- Service health grid (Ollama, APIs, web services)
- Claude Code activity feed
- Factory runs and agent swarms
- Bus vitals and drift metrics
- Calendar and priorities
- Open loops tracker
- Evolution velocity

### System Map
Network topology dashboard with:
- All services by tier (inference, core, agent, public)
- Port listeners and health checks
- LaunchAgent status
- Real-time service monitoring

## Installation

```bash
pip install mirrordna-dashboards
```

## Usage

```bash
# Launch cognitive dashboard
cognitive-dashboard

# Launch system map
system-map

# Single snapshot (no live refresh)
system-map --once
```

## Requirements

- Python 3.9+
- Terminal with Unicode support
- (Optional) MirrorDNA infrastructure for full feature set

## Standalone Mode

The dashboards work in two modes:
1. **Full MirrorDNA mode**: When running on a MirrorDNA system, reads live data from bus, vault, and service registry
2. **Standalone mode**: When infrastructure is not present, shows mock data for demonstration

## Configuration

Dashboards auto-detect MirrorDNA paths:
- `~/.mirrordna/` - Configuration and bus
- `~/MirrorDNA-Vault/` - Vault storage
- `~/.mirrordna/SERVICE_REGISTRY.json` - Service definitions

If these paths don't exist, dashboards run in demo mode.

## Architecture

Built for sovereign AI systems:
- No cloud dependencies
- Reads local filesystem and HTTP endpoints
- Real-time terminal rendering via Rich
- Minimal resource footprint

## License

MIT License - see LICENSE file

## Author

Paul Desai ([@activemirror](https://github.com/activemirror))

Part of the ActiveMirror ecosystem - Reflective AI Infrastructure.

## Links

- [ActiveMirror](https://activemirror.ai)
- [Documentation](https://docs.activemirror.ai)
- [GitHub](https://github.com/activemirror/mirrordna-dashboards)
