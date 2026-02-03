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

def list_workspaces() -> str:
    """List all Power BI workspaces the user has access to."""
    logger.info("TOOL CALLED: list_workspaces()")
    try:
        df = _pbi_client.list_workspaces()
        logger.info(f"list_workspaces returned {len(df)} workspaces")
        result = df[["name", "id"]].to_string(index=False)
        logger.debug(f"Result: {result[:200]}...")
        return result
    except Exception as e:
        logger.error(f"list_workspaces FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error listing workspaces: {str(e)}"


def list_datasets(workspace_name: str) -> str:
    """List all datasets in a Power BI workspace."""
    logger.info(f"TOOL CALLED: list_datasets(workspace_name='{workspace_name}')")
    try:
        df = _pbi_client.list_datasets(workspace_name)
        logger.info(f"list_datasets returned {len(df)} datasets")
        result = df[["name", "id"]].to_string(index=False)
        return result
    except Exception as e:
        logger.error(f"list_datasets FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error listing datasets: {str(e)}"


def describe_dataset(workspace_name: str, dataset_name: str) -> str:
    """Get the schema of a Power BI semantic model including tables, columns, and relationships."""
    logger.info(f"TOOL CALLED: describe_dataset(workspace='{workspace_name}', dataset='{dataset_name}')")
    try:
        schema = _pbi_client.describe_dataset(workspace_name, dataset_name)
        logger.info(f"describe_dataset returned schema with {len(schema.get('tables', []))} tables")
        return schema["llm_context"]
    except Exception as e:
        logger.error(f"describe_dataset FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error describing dataset: {str(e)}"


def execute_dax(workspace_name: str, dataset_name: str, dax_query: str) -> str:
    """Execute a DAX query against a Power BI semantic model and return results."""
    logger.info(f"TOOL CALLED: execute_dax(workspace='{workspace_name}', dataset='{dataset_name}')")
    logger.debug(f"DAX Query: {dax_query}")
    try:
        df = _pbi_client.execute_dax(workspace_name, dataset_name, dax_query)
        if df.empty:
            logger.info("execute_dax returned no results")
            return "Query returned no results."
        logger.info(f"execute_dax returned {len(df)} rows")
        return df.to_string(index=False)
    except Exception as e:
        logger.error(f"execute_dax FAILED: {e}")
        logger.error(traceback.format_exc())
        return f"Error executing DAX: {str(e)}"


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

# Create the agent with tools
agent = ChatAgent(
    name="PowerBIAgent",
    instructions="""You are a helpful Power BI analyst assistant. You can:
1. List workspaces and datasets
2. Describe semantic model schemas
3. Write and execute DAX queries to answer user questions

When answering questions about data:
- First describe the dataset schema to understand the model
- Write appropriate DAX queries
- Present results clearly with insights

Be concise and helpful.""",
    chat_client=llm_client,
    tools=[list_workspaces, list_datasets, describe_dataset, execute_dax],
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
