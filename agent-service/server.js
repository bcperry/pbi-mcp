import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { MultiServerMCPClient } from "@langchain/mcp-adapters";
import { ChatOpenAI } from "@langchain/openai";
import { createAgent } from "langchain";

dotenv.config({ path: "../.env" });

const app = express();
app.use(cors({
  origin: true,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));
app.use(express.json({ limit: "2mb" }));

const MCP_URL = process.env.MCP_URL || "http://localhost:8000/mcp";
const PORT = process.env.AGENT_PORT || 7071;

const AZURE_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZURE_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const AZURE_MODEL = process.env.AZURE_OPENAI_MODEL;
const AZURE_API_VERSION = process.env.AZURE_OPENAI_API_VERSION;

if (!AZURE_ENDPOINT || !AZURE_API_KEY || !AZURE_MODEL || !AZURE_API_VERSION) {
  console.error("Missing Azure OpenAI env vars. Check .env.");
}

let agent = null;
let client = null;

async function initAgent() {
  client = new MultiServerMCPClient({
    powerbi: {
      transport: "http",
      url: MCP_URL,
    },
  });

  const tools = await client.getTools();

  const instanceName = new URL(AZURE_ENDPOINT).hostname.split(".")[0];
  const model = new ChatOpenAI({
    azureOpenAIApiKey: AZURE_API_KEY,
    azureOpenAIApiVersion: AZURE_API_VERSION,
    azureOpenAIApiInstanceName: instanceName,
    azureOpenAIApiDeploymentName: AZURE_MODEL,
  });

  agent = createAgent({ model, tools });
}

app.get("/health", async (_req, res) => {
  try {
    if (!agent) await initAgent();
    res.json({ ok: true, mcp: MCP_URL });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/chat", async (req, res) => {
  try {
    if (!agent) await initAgent();

    const { messages, context } = req.body || {};
    if (!Array.isArray(messages)) {
      return res.status(400).json({ error: "messages must be an array" });
    }

    const systemMsg = {
      role: "system",
      content: `You are a Power BI data analyst assistant. Use MCP tools to access semantic model data.\nContext: ${JSON.stringify(
        context || {}
      )}`,
    };

    const response = await agent.invoke({
      messages: [systemMsg, ...messages],
    });

    const finalMessage =
      response?.messages?.[response.messages.length - 1]?.content ||
      response?.output ||
      response?.content ||
      "(No response)";

    res.json({ message: finalMessage });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

app.listen(PORT, () => {
  console.log(`Agent service running on http://localhost:${PORT}`);
  console.log(`Using MCP server at ${MCP_URL}`);
});
