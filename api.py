"""
Simple FastAPI server with Azure OpenAI agent for Power BI semantic model queries.
Uses agent-framework for agent orchestration.
"""

import os
import logging
import traceback
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from azure.identity import DefaultAzureCredential, DeviceCodeCredential, AzureCliCredential
from azure.core.exceptions import ClientAuthenticationError

from agent_framework.azure import AzureOpenAIChatClient
from agent_framework import ChatAgent, ChatMessage, Role, TextContent
from powerbi_client import PowerBIClient

# Configure logging to work with uvicorn
import uvicorn.logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:     %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("pbi-agent")
logger.setLevel(logging.INFO)

# Load environment variables
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Authenticate at startup (like the notebook)
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Authenticating to Power BI at startup...")
try:
    # Use AzureCliCredential directly (from az login)
    _credential = AzureCliCredential()
    _token = _credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    logger.info("✓ Authenticated using AzureCliCredential (az login)")
except Exception as e:
    logger.warning(f"AzureCliCredential failed: {e}")
    try:
        _credential = DefaultAzureCredential()
        _token = _credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        logger.info("✓ Authenticated using DefaultAzureCredential")
    except ClientAuthenticationError as e2:
        logger.error(f"DefaultAzureCredential also failed: {e2}")
        logger.info("Using DeviceCodeCredential - you will need to authenticate on first request")
        _credential = DeviceCodeCredential()

# Create the Power BI client with the authenticated credential
_pbi_client = PowerBIClient(_credential)
logger.info("Power BI client initialized")

# ─────────────────────────────────────────────────────────────────────────────
# Discover workspace and dataset at startup
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Discovering workspace and dataset...")
_workspaces_df = _pbi_client.list_workspaces()
if _workspaces_df.empty:
    raise RuntimeError("No workspaces found")
WORKSPACE_NAME = _workspaces_df.iloc[0]["name"]
logger.info(f"Using workspace: {WORKSPACE_NAME}")

_datasets_df = _pbi_client.list_datasets(WORKSPACE_NAME)
if _datasets_df.empty:
    raise RuntimeError(f"No datasets found in workspace '{WORKSPACE_NAME}'")
DATASET_NAME = _datasets_df.iloc[0]["name"]
logger.info(f"Using dataset: {DATASET_NAME}")

# Load schema once at startup
logger.info("Loading dataset schema...")
_schema = _pbi_client.describe_dataset(WORKSPACE_NAME, DATASET_NAME)
DATASET_SCHEMA = _schema["llm_context"]
logger.info(f"Schema loaded: {len(_schema.get('tables', []))} tables")

# Initialize FastAPI
app = FastAPI(title="Power BI Agent API")

# CORS for Power BI visual
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Tool Functions - These will be available to the agent
# ─────────────────────────────────────────────────────────────────────────────

def execute_dax(dax_query: str) -> str:
    """Execute a DAX query against the semantic model and return results."""
    logger.info(f"TOOL CALLED: execute_dax()")
    logger.debug(f"DAX Query: {dax_query}")
    try:
        df = _pbi_client.execute_dax(WORKSPACE_NAME, DATASET_NAME, dax_query)
        if df.empty:
            logger.info("execute_dax returned no results")
            return "Query returned no results."
        logger.info(f"execute_dax returned {len(df)} rows")
        return df.to_string(index=False)
    except Exception as e:
        logger.error(f"execute_dax FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error executing DAX: {str(e)}"


def get_table_sample(table_name: str, num_rows: int = 5) -> str:
    """Get sample rows from a table to understand its data content. Use this to see what values look like before writing complex queries."""
    logger.info(f"TOOL CALLED: get_table_sample(table='{table_name}', rows={num_rows})")
    try:
        dax_query = f"EVALUATE TOPN({num_rows}, '{table_name}')"
        df = _pbi_client.execute_dax(WORKSPACE_NAME, DATASET_NAME, dax_query)
        if df.empty:
            return f"Table '{table_name}' is empty."
        logger.info(f"get_table_sample returned {len(df)} rows")
        return df.to_string(index=False)
    except Exception as e:
        logger.error(f"get_table_sample FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error sampling table: {str(e)}"


def search_across_tables(search_term: str) -> str:
    """Search for a value across all text columns in all tables. Returns matching rows from any table containing the search term. Use this to find where specific names, IDs, or values exist in the model."""
    logger.info(f"TOOL CALLED: search_across_tables(search='{search_term}')")
    try:
        # Get schema to find all tables and text columns
        schema = _pbi_client.describe_dataset(WORKSPACE_NAME, DATASET_NAME)
        results = []
        
        for table in schema.get("tables", []):
            table_name = table["name"]
            text_columns = [col["name"] for col in table.get("columns", []) 
                          if col.get("dataType", "").lower() in ("string", "text")]
            
            if not text_columns:
                continue
            
            # Build OR condition for all text columns
            conditions = " || ".join([f'CONTAINSSTRING([{col}], "{search_term}")' for col in text_columns])
            dax_query = f"EVALUATE FILTER('{table_name}', {conditions})"
            
            try:
                df = _pbi_client.execute_dax(WORKSPACE_NAME, DATASET_NAME, dax_query)
                if not df.empty:
                    results.append(f"=== {table_name} ({len(df)} matches) ===\n{df.to_string(index=False)}")
            except Exception as table_error:
                logger.warning(f"Search in {table_name} failed: {table_error}")
                continue
        
        if results:
            return "\n\n".join(results)
        else:
            return f"No matches found for '{search_term}' in any table."
    except Exception as e:
        logger.error(f"search_across_tables FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error searching tables: {str(e)}"


# ─────────────────────────────────────────────────────────────────────────────
# Agent Setup
# ─────────────────────────────────────────────────────────────────────────────

# Create the Azure OpenAI client
llm_client = AzureOpenAIChatClient(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-01-preview"),
    deployment_name=os.getenv("AZURE_OPENAI_MODEL", "gpt-4o"),
)

# Create the agent with tools - schema is injected at startup
AGENT_INSTRUCTIONS = f"""You are a Power BI analyst. Answer questions by querying the semantic model below. Be concise - give results, not explanations of your process.

## DATASET SCHEMA

{DATASET_SCHEMA}

## RULES

1. **ACT, DON'T ASK** - Never ask permission. Just query and answer.

2. **USE EXACT NAMES** - Use ONLY the table/column names from the schema above. Never guess or invent names.

3. **BE PERSISTENT** - If a query returns nothing, try other tables. Use search_across_tables for finding specific values. Keep going until you find the answer or exhaust all options.

4. **BE BRIEF** - Report findings directly. Skip the narration about your approach.

## TOOLS

- get_table_sample: Preview rows from a table to understand its data
- search_across_tables: Find a value across all text columns (use for names, IDs, etc.)
- execute_dax: Run DAX queries

## DAX SYNTAX

- Use exact names from schema: EVALUATE 'Customer' or EVALUATE FILTER('Customer', [Name] = "X")
- Text search: FILTER('Table', CONTAINSSTRING([Column], "term"))
- Limit results: TOPN(10, 'Table')"""

agent = ChatAgent(
    name="PowerBIAgent",
    instructions=AGENT_INSTRUCTIONS,
    chat_client=llm_client,
    tools=[execute_dax, get_table_sample, search_across_tables],
)


# ─────────────────────────────────────────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────────────────────────────────────────

class Message(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[Message] = []


class ChatResponse(BaseModel):
    reply: str


# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Chat with the Power BI agent.
    
    Args:
        request: Contains the new message and optional conversation history
        
    Returns:
        The agent's reply
    """
    logger.info(f"=== CHAT REQUEST ===")
    logger.info(f"User message: {request.message}")
    logger.info(f"History length: {len(request.history)}")
    
    try:
        # Build messages list from history
        messages: list[ChatMessage] = []
        
        for msg in request.history:
            if msg.role == "user":
                messages.append(ChatMessage(role=Role.USER, contents=[TextContent(text=msg.content)]))
            elif msg.role == "assistant":
                messages.append(ChatMessage(role=Role.ASSISTANT, contents=[TextContent(text=msg.content)]))
        
        # Add the new user message
        messages.append(ChatMessage(role=Role.USER, contents=[TextContent(text=request.message)]))
        
        logger.info(f"Running agent with {len(messages)} messages...")
        
        # Run the agent with messages
        response = await agent.run(messages=messages)
        
        logger.info(f"Agent response type: {type(response)}")
        logger.debug(f"Agent response: {response}")
        
        # Extract the reply text
        reply_text = response.reply if hasattr(response, 'reply') else str(response)
        
        logger.info(f"Reply: {reply_text[:200]}...")
        
        return ChatResponse(reply=reply_text)
    except Exception as e:
        logger.error(f"CHAT ENDPOINT FAILED: {e}")
        logger.error(traceback.format_exc())
        return ChatResponse(reply=f"Error: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
