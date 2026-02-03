"""Power BI MCP Server - Access semantic models via FastMCP."""

from fastmcp import FastMCP
from pydantic import Field
from azure.identity import DefaultAzureCredential
from starlette.middleware import Middleware
import logging

from powerbi_client import PowerBIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP(name="Power BI MCP Server")

# Initialize Power BI client
_client = None


class RequestIPLogger:
    """ASGI middleware to log client IPs for HTTP requests."""

    def __init__(self, app, logger: logging.Logger):
        self._app = app
        self._logger = logger

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            client = scope.get("client") or ("unknown", 0)
            method = scope.get("method", "?")
            path = scope.get("path", "?")
            self._logger.info("HTTP %s %s from %s:%s", method, path, client[0], client[1])
        await self._app(scope, receive, send)


def get_client() -> PowerBIClient:
    """Get or create the Power BI client."""
    global _client
    if _client is None:
        credential = DefaultAzureCredential()
        _client = PowerBIClient(credential)
        logger.info("PowerBIClient initialized")
    return _client


# =============================================================================
# TOOLS
# =============================================================================

@mcp.tool
def list_workspaces() -> list[dict]:
    """List all Power BI workspaces accessible to the authenticated user."""
    client = get_client()
    df = client.list_workspaces()
    return [
        {"name": row["name"], "id": row["id"], "is_premium": row.get("isOnDedicatedCapacity", False)} 
        for _, row in df.iterrows()
    ]


@mcp.tool
def list_datasets(workspace_name: str = Field(description="Name of the Power BI workspace")) -> list[dict]:
    """List all semantic models (datasets) in a workspace."""
    client = get_client()
    df = client.list_datasets(workspace_name)
    return [{"name": row["name"], "id": row["id"]} for _, row in df.iterrows()]


@mcp.tool
def read_table(
    workspace_name: str = Field(description="Name of the Power BI workspace"),
    dataset_name: str = Field(description="Name of the semantic model"),
    table_name: str = Field(description="Name of the table to read"),
    top_n: int = Field(default=100, description="Maximum rows to return")
) -> list[dict]:
    """Read data from a table in a semantic model."""
    client = get_client()
    df = client.read_table(workspace_name, dataset_name, table_name, top_n=top_n)
    return df.to_dict(orient="records")


@mcp.tool
def execute_dax_query(
    workspace_name: str = Field(description="Name of the Power BI workspace"),
    dataset_name: str = Field(description="Name of the semantic model"),
    dax_query: str = Field(description="DAX query to execute (must start with EVALUATE)")
) -> list[dict]:
    """Execute a DAX query against a Power BI semantic model."""
    client = get_client()
    df = client.execute_dax(workspace_name, dataset_name, dax_query)
    return df.to_dict(orient="records")


@mcp.tool
def describe_dataset(
    workspace_name: str = Field(description="Name of the Power BI workspace"),
    dataset_name: str = Field(description="Name of the semantic model")
) -> dict:
    """Get complete schema information about a semantic model including all tables, columns, and relationships.
    
    Use this tool first to understand the data model before writing DAX queries.
    Returns table names, column names, data types, cardinality, and an LLM-friendly context string.
    """
    client = get_client()
    result = client.describe_dataset(workspace_name, dataset_name)
    
    # Add usage hint
    result["usage_hint"] = (
        "Use table and column names in DAX queries like: "
        "EVALUATE TOPN(10, 'TableName') or "
        "EVALUATE SUMMARIZECOLUMNS('Table'[Column], \"Measure\", SUM('Table'[Value]))"
    )
    
    return result


@mcp.tool
def search_table(
    workspace_name: str = Field(description="Name of the Power BI workspace"),
    dataset_name: str = Field(description="Name of the semantic model"),
    table_name: str = Field(description="Name of the table to search"),
    column_name: str = Field(description="Column to search in"),
    search_value: str = Field(description="Value to search for"),
    max_rows: int = Field(default=100, description="Maximum rows to return")
) -> list[dict]:
    """Search for rows in a table where a column contains a value."""
    client = get_client()
    dax = f"""
EVALUATE
TOPN({max_rows}, FILTER('{table_name}', CONTAINSSTRING('{table_name}'[{column_name}], "{search_value}")))
"""
    df = client.execute_dax(workspace_name, dataset_name, dax)
    return df.to_dict(orient="records")


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8000,
        middleware=[Middleware(RequestIPLogger, logger=logger)],
    )
