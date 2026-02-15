[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE.md)

# Vero

**Vero** is a modern, open-source analytics and AI orchestration stack — designed to help organizations work **data-driven** and **agent-driven** without relying on black-box SaaS platforms or heavyweight enterprise BI tools.

It unifies best-in-class open technologies into a modular architecture that enables you to **ingest, model, visualize, and query data** — via both **interactive dashboards** and **natural language agents**.

But Vero goes beyond analytics.

It introduces support for **Agentic AI**, enabling **multi-agent workflows**, **dynamic orchestration**, and **performance-aware coordination** — allowing AI systems to take real action in complex environments while remaining observable, auditable, and governed.

![Vero Stack](/docs/images/vero-stack.png)


## Why Vero?

> **Make it easy for small and mid-sized teams to adopt a modern, open, and self-hosted analytics + AI stack — without giving up control.**

### Common Pain Points

- Opaque vendor platforms with unclear logic
- Complex, inflexible data tools that resist integration
- Data sent off to third-party clouds just to get basic metrics
- No insight into what your AI agents are doing — or why
- Difficulty scaling AI workflows beyond one-off scripts

## What Vero Solves

Vero was built to address these challenges with a **clean, composable reference architecture** that combines modern analytics with intelligent, governed agent orchestration.

### Our Goals

- **Agent-first orchestration**: Native support for **multi-agent environments** with **workflow automation**, **decision delegation**, and **performance-aware orchestration**
- **Self-hosted analytics**: Run the entire stack on your own infra — no vendor lock-in, no hidden fees
- **Data sovereignty**: Your data stays where it belongs — inside your infrastructure
- **Modular & modern**: Built on today's best open-source tools (dlt, BSL, Metabase, n8n, Streamlit, DuckDB, etc.)
- **Natural language interfaces**: Enable users to ask complex questions — and get answers — through integrated LLM agents
- **Scalable architecture**: Designed for environments that need both **real-time insights** and **AI actionability**
- **Team-friendly**: Built to support engineers, analysts, and domain experts equally — no specialization required

With Vero, you're not just consuming data — you're building **intelligent systems** that reason, decide, and act.

And you own the full pipeline.

From **ingestion to visualization**, from **raw facts to orchestrated agents**, Vero gives your team the **clarity, control, and composability** to move from **insight to action** — securely, transparently, and at scale.

## Architecture Overview

Vero is made up of six layers:

1. **Data Ingestion** – powered by [dlt](https://dlthub.com) (data load tool)
2. **Data Warehouse** – DuckDB (local, file-based) with PostgreSQL for persistence
3. **Semantic Modeling** – [BSL](https://github.com/dlt-hub/boring-semantic-layer) (Boring Semantic Layer) with Ibis
4. **AI Agent Interface** – [Agno](https://docs.agno.com/introduction) + MCP server
5. **Agentic AI Workflow Orchestration** – [n8n](https://docs.n8n.io/) for automating agentic tasks and follow-ups
6. **BI & Dashboards** – [Metabase](https://metabase.com) (optional: Superset, Tableau, Power BI, etc.)

Each layer is loosely coupled and can be replaced or extended as needed.

## What's Included?

- Prebuilt **Docker environment** for local or on-prem deployment
- Sample dataset based on the **Contoso Retail** model
- **dlt pipeline** for loading CSV data into DuckDB
- **BSL semantic model** with dimensions, measures, and star-schema joins
- Ready-to-use **MCP server** for AI agent queries via the semantic layer
- **Streamlit KPI Explorer** for interactive data exploration
- **FastAPI** REST endpoints for programmatic semantic queries
- Natural language query agent powered by **GPT** or **Claude**
- Automated workflows and agentic AI orchestration with **n8n**
- Traditional dashboards and visualizations via **Metabase**
- **NGINX reverse proxy** setup for routing and traffic management
- **Let's Encrypt SSL certificates** automated via **nginx-proxy** and **docker-gen** for secure HTTPS out of the box

## Quickstart

```bash
# 1. Clone the repository
$ git clone https://github.com/njaltran/Vero-dlt.git
$ cd Vero-dlt

# 2. Install Python dependencies
$ pip install -e .
# or with uv:
$ uv sync

# 3. Run the dlt pipeline to load data into DuckDB
$ python pipeline.py

# 4. Verify the semantic model
$ python -m semantics.model

# 5. Launch the Streamlit KPI Explorer
$ streamlit run downstream_apps/kpi_explorer.py

# 6. Or start the FastAPI server
$ python -m downstream_apps.api.server
```

### Docker Compose

Choose the right Docker Compose file for your architecture:

| File                        | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `docker-compose.yaml`       | Default version for most x86_64 environments                 |
| `docker-compose.arm64.yaml` | Special version for ARM-based systems like Apple Silicon (M1/M2 chips) |

```bash
# For x86_64:
docker compose --env-file .env.dev -f docker-compose.yaml up --build

# For ARM64 (Apple Silicon):
docker compose --env-file .env.dev -f docker-compose.arm64.yaml up --build
```

> See [`docs/quickstart.md`](./docs/quickstart.md) for full instructions and environment setup.

## Documentation

- [Architecture Overview](./docs/architecture.md)
- [Quickstart](./docs/quickstart.md)
- [Docker & Runtime Config](./docs/conf/docker.md)
- [Environment Variables](./docs/conf/environment.md)
- [Semantic Modeling (BSL)](./docs/semantic/bsl.md)
- [BI Layer (Metabase)](./docs/bi/metabase.md)
- [AI Agent Layer (MCP + Agno)](./docs/ai-agent/mcp-server.md)
- [Data Ingestion (dlt)](./docs/ingestion/dlt.md)
- [Warehouse Layer (PostgreSQL)](./docs/warehouse/postgres.md)


## Contributions Welcome

This is a community-friendly reference project. We welcome:

- Feature suggestions and bug reports
- Docs, UX, and onboarding improvements
- New data sources, transformations, and AI plugins

Feel free to open a PR or issue!

## License

Vero is licensed under the GPL-3.0 License. All included third-party projects follow their respective licenses.

For support, ideas, or integration help, reach out via GitHub Discussions or open an issue. We're building Vero to be the foundation for modern, self-managed analytics — for everyone.
