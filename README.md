# Power BI MCP Server

An MCP (Model Context Protocol) server that provides access to Power BI semantic models, enabling AI assistants to query and explore your Power BI data.

## Features

- **List Workspaces & Datasets**: Discover available Power BI workspaces and semantic models
- **Explore Schema**: View tables, columns, and measures in a semantic model
- **Execute DAX Queries**: Run custom DAX queries against semantic models
- **Search Data**: Search for values within tables
- **Evaluate Measures**: Calculate measure expressions with optional grouping

## Prerequisites

1. **Azure CLI**: Install and authenticate with `az login`
2. **Power BI Premium/PPU/Fabric**: Required for DAX query execution
3. **Workspace Access**: Read access to target workspaces and datasets

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd pbi-mcp

# Install with uv
uv sync

# Or install in editable mode
uv pip install -e .
```

## Usage

### Development Mode (with Inspector)

```bash
uv run fastmcp dev src/pbi_mcp/server.py
```

### Run as MCP Server

```bash
uv run pbi-mcp
```

### Configure in Claude Desktop

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "uv",
      "args": ["--directory", "/path/to/pbi-mcp", "run", "pbi-mcp"]
    }
  }
}
```

## Available Tools

| Tool | Description |
|------|-------------|
| `execute_dax_query` | Execute a custom DAX query against a semantic model |
| `search_table_data` | Search for values in a specific table column |
| `read_table_data` | Read rows from a table |
| `evaluate_measure` | Evaluate a DAX measure expression |

## Available Resources

| Resource URI | Description |
|--------------|-------------|
| `powerbi://workspaces` | List all accessible workspaces |
| `powerbi://workspaces/{name}/datasets` | List datasets in a workspace |
| `powerbi://workspaces/{ws}/datasets/{ds}/tables` | List tables in a dataset |
| `powerbi://workspaces/{ws}/datasets/{ds}/measures` | List measures in a dataset |
| `powerbi://workspaces/{ws}/datasets/{ds}/tables/{tbl}/schema` | Get table schema |

## Available Prompts

| Prompt | Description |
|--------|-------------|
| `analyze_dataset` | Analyze a semantic model's structure |
| `search_data` | Search for a term across a dataset |
| `generate_dax_query` | Generate a DAX query from natural language |

## Authentication

The server uses Azure Identity's `DefaultAzureCredential`, which supports:
- Azure CLI authentication (`az login`)
- Environment variables
- Managed Identity (in Azure)
- Visual Studio Code authentication

## License

MIT
