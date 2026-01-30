"""Power BI Client for interacting with semantic models via REST API."""

import pandas as pd
import requests


class PowerBIClient:
    """Client for interacting with Power BI semantic models via REST API."""
    
    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    
    def __init__(self, credential):
        """
        Initialize the Power BI client.
        
        Args:
            credential: Azure credential object (e.g., DefaultAzureCredential, DeviceCodeCredential)
        """
        self.credential = credential
        self._workspaces_cache = None
    
    def _get_headers(self):
        """Get authorization headers for API requests."""
        token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json"
        }
    
    def list_workspaces(self) -> pd.DataFrame:
        """List all workspaces the user has access to."""
        response = requests.get(f"{self.BASE_URL}/groups", headers=self._get_headers())
        response.raise_for_status()
        workspaces = response.json().get("value", [])
        self._workspaces_cache = {ws["name"]: ws for ws in workspaces}
        return pd.DataFrame(workspaces)
    
    def get_workspace_id(self, workspace_name: str) -> str:
        """Get workspace ID by name."""
        if not self._workspaces_cache:
            self.list_workspaces()
        ws = self._workspaces_cache.get(workspace_name)
        if not ws:
            raise ValueError(f"Workspace '{workspace_name}' not found")
        return ws["id"]
    
    def is_premium(self, workspace_name: str) -> bool:
        """Check if workspace is on Premium/Fabric capacity."""
        if not self._workspaces_cache:
            self.list_workspaces()
        ws = self._workspaces_cache.get(workspace_name)
        return ws.get("isOnDedicatedCapacity", False) if ws else False
    
    def list_datasets(self, workspace_name: str) -> pd.DataFrame:
        """List all datasets in a workspace."""
        workspace_id = self.get_workspace_id(workspace_name)
        url = f"{self.BASE_URL}/groups/{workspace_id}/datasets"
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return pd.DataFrame(response.json().get("value", []))
    
    def get_dataset_id(self, workspace_name: str, dataset_name: str) -> str:
        """Get dataset ID by name."""
        df = self.list_datasets(workspace_name)
        match = df[df["name"] == dataset_name]
        if match.empty:
            raise ValueError(f"Dataset '{dataset_name}' not found in workspace '{workspace_name}'")
        return match.iloc[0]["id"]
    
    def execute_dax(self, workspace_name: str, dataset_name: str, dax_query: str) -> pd.DataFrame:
        """Execute a DAX query and return results as DataFrame."""
        workspace_id = self.get_workspace_id(workspace_name)
        dataset_id = self.get_dataset_id(workspace_name, dataset_name)
        
        url = f"{self.BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        
        if response.status_code != 200:
            error = response.json().get("error", {})
            raise RuntimeError(f"DAX query failed: {error.get('message', response.text)}")
        
        result = response.json()
        tables = result.get("results", [{}])[0].get("tables", [])
        if tables:
            rows = tables[0].get("rows", [])
            return pd.DataFrame(rows)
        return pd.DataFrame()
    
    def read_table(self, workspace_name: str, dataset_name: str, table_name: str, 
                   top_n: int = None, columns: list = None) -> pd.DataFrame:
        """Read data from a table in a semantic model."""
        col_list = ", ".join(columns) if columns else "*"
        
        if top_n:
            dax = f"EVALUATE TOPN({top_n}, '{table_name}')"
        else:
            dax = f"EVALUATE '{table_name}'"
        
        return self.execute_dax(workspace_name, dataset_name, dax)
    
    def evaluate_measure(self, workspace_name: str, dataset_name: str, 
                         measure: str, group_by: list = None) -> pd.DataFrame:
        """Evaluate a measure, optionally grouped by columns."""
        if group_by:
            cols = ", ".join(group_by)
            dax = f"""
            EVALUATE 
            SUMMARIZECOLUMNS(
                {cols},
                "Result", {measure}
            )
            """
        else:
            dax = f"EVALUATE ROW(\"Result\", {measure})"
        
        return self.execute_dax(workspace_name, dataset_name, dax)
    
    def describe_dataset(self, workspace_name: str, dataset_name: str) -> dict:
        """
        Get complete schema information about a semantic model for LLM consumption.
        
        Returns tables, columns with data types, sample values, cardinality, and relationships.
        
        Args:
            workspace_name: Name of the Power BI workspace
            dataset_name: Name of the semantic model/dataset
            
        Returns:
            dict with keys: dataset_name, dataset_id, tables, relationships, llm_context
        """
        dataset_id = self.get_dataset_id(workspace_name, dataset_name)
        
        # 1. Get column statistics (table names, column names, min/max values, cardinality)
        schema_dax = "EVALUATE COLUMNSTATISTICS()"
        df_schema = self.execute_dax(workspace_name, dataset_name, schema_dax)
        
        # 2. Build the schema dictionary from COLUMNSTATISTICS results
        tables_dict = {}
        for _, row in df_schema.iterrows():
            table_name = row.get("[Table Name]") or row.get("Table Name")
            col_name = row.get("[Column Name]") or row.get("Column Name")
            
            # Skip auto-generated date tables and internal RowNumber columns
            if table_name and ("DateTableTemplate" in table_name or "LocalDateTable" in table_name):
                continue
            if col_name and "RowNumber-" in col_name:
                continue
            
            if table_name not in tables_dict:
                tables_dict[table_name] = {"name": table_name, "columns": []}
            
            # Extract all available column metadata
            min_val = row.get("[Min]") or row.get("Min")
            max_val = row.get("[Max]") or row.get("Max")
            cardinality = row.get("[Cardinality]") or row.get("Cardinality")
            
            # Infer data type from min/max values
            data_type = "Unknown"
            if min_val is not None:
                if isinstance(min_val, (int, float)):
                    data_type = "Number"
                elif isinstance(min_val, str):
                    # Check if it looks like a date
                    if any(c in str(min_val) for c in ["-", "/", ":"]) and len(str(min_val)) >= 8:
                        data_type = "DateTime"
                    else:
                        data_type = "Text"
            
            tables_dict[table_name]["columns"].append({
                "name": col_name,
                "dataType": data_type,
                "minValue": min_val,
                "maxValue": max_val,
                "cardinality": cardinality,
            })
        
        tables = list(tables_dict.values())
        
        # 3. Get relationships by examining key columns (columns ending in Key, ID, etc.)
        relationships = []
        key_columns = {}
        for table in tables:
            for col in table["columns"]:
                col_name = col["name"]
                if col_name and (col_name.endswith(" Key") or col_name.endswith("_Key") or 
                               col_name.endswith(" ID") or col_name.endswith("_ID") or
                               col_name.endswith("_id") or col_name == "ID"):
                    key_columns.setdefault(col_name, []).append(table["name"])
        
        # Infer relationships from matching key columns across tables
        for key_name, table_list in key_columns.items():
            if len(table_list) > 1:
                relationships.append({
                    "keyColumn": key_name,
                    "tables": table_list
                })
        
        # 4. Generate LLM-friendly context string
        llm_lines = [
            f"# Power BI Semantic Model: {dataset_name}",
            "",
            "## Overview",
            f"This model contains {len(tables)} tables that can be queried using DAX.",
            "",
            "## Tables and Columns"
        ]
        
        for table in tables:
            llm_lines.append(f"\n### '{table['name']}'")
            llm_lines.append("| Column | Type | Cardinality | Sample Range |")
            llm_lines.append("|--------|------|-------------|--------------|")
            for col in table["columns"]:
                card = col.get('cardinality', 'N/A')
                min_v = col.get('minValue', '')
                max_v = col.get('maxValue', '')
                sample = f"{min_v} to {max_v}" if min_v and max_v else "N/A"
                llm_lines.append(f"| {col['name']} | {col.get('dataType', 'Unknown')} | {card} | {sample} |")
        
        if relationships:
            llm_lines.append("\n## Inferred Relationships")
            llm_lines.append("The following key columns appear in multiple tables, suggesting relationships:")
            for rel in relationships:
                llm_lines.append(f"- **{rel['keyColumn']}**: links {' <-> '.join(rel['tables'])}")
        
        llm_lines.extend([
            "",
            "## DAX Query Tips",
            "- Use EVALUATE to return a table",
            "- Reference tables with single quotes: 'TableName'",
            "- Reference columns as 'Table'[Column]",
            "- Use SUMMARIZECOLUMNS for grouping and aggregation",
            "- Use TOPN for limiting results"
        ])
        
        llm_context = "\n".join(llm_lines)
        
        return {
            "dataset_name": dataset_name,
            "dataset_id": dataset_id,
            "tables": tables,
            "relationships": relationships,
            "llm_context": llm_context
        }
