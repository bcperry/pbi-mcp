/**
 * Power BI Chat Visual
 * Connects to FastAPI agent for Power BI data access
 */

"use strict";

import powerbi from "powerbi-visuals-api";
import { FormattingSettingsService } from "powerbi-visuals-utils-formattingmodel";
import "./../style/visual.less";

import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisual = powerbi.extensibility.visual.IVisual;

import { VisualFormattingSettingsModel } from "./settings";

// ============================================================================
// TYPES
// ============================================================================

interface ChatMessage {
    role: 'user' | 'assistant';
    content: string;
}

interface ChatRequest {
    message: string;
    history: ChatMessage[];
}

interface ChatResponse {
    reply: string;
}

// ============================================================================
// POWER BI VISUAL CLASS
// ============================================================================

export class Visual implements IVisual {
    private target: HTMLElement;
    private formattingSettings: VisualFormattingSettingsModel;
    private formattingSettingsService: FormattingSettingsService;
    private chatContainer: HTMLDivElement;
    private messagesContainer: HTMLDivElement;
    private textInput: HTMLTextAreaElement;
    private sendButton: HTMLButtonElement;
    private statusBanner: HTMLDivElement;
    private history: ChatMessage[] = [];
    private serverUrl: string = "http://localhost:8000";

    constructor(options: VisualConstructorOptions) {
        console.log("Power BI Chat Visual initializing");
        this.formattingSettingsService = new FormattingSettingsService();
        this.target = options.element;
        this.createUI();
        this.checkHealth();
    }

    private async checkHealth(): Promise<void> {
        try {
            this.setStatus("Connecting...", "info");
            const response = await fetch(`${this.serverUrl}/health`);
            if (response.ok) {
                this.setStatus("Connected", "success");
            } else {
                throw new Error(`Status ${response.status}`);
            }
        } catch (error) {
            this.setStatus("Server offline. Click to retry.", "error");
            setTimeout(() => this.checkHealth(), 5000);
        }
    }

    private createUI(): void {
        this.chatContainer = document.createElement("div");
        this.chatContainer.className = "chat-container";

        // Header
        const header = document.createElement("div");
        header.className = "chat-header";
        header.textContent = "🤖 Power BI Assistant";
        this.chatContainer.appendChild(header);

        // Status
        this.statusBanner = document.createElement("div");
        this.statusBanner.className = "chat-status info";
        this.statusBanner.textContent = "Initializing...";
        this.statusBanner.onclick = () => this.checkHealth();
        this.chatContainer.appendChild(this.statusBanner);

        // Messages
        this.messagesContainer = document.createElement("div");
        this.messagesContainer.className = "chat-messages";
        this.chatContainer.appendChild(this.messagesContainer);

        // Input area
        const inputContainer = document.createElement("div");
        inputContainer.className = "chat-input-container";

        this.textInput = document.createElement("textarea");
        this.textInput.className = "chat-input";
        this.textInput.placeholder = "Ask about your Power BI data...";
        this.textInput.rows = 2;
        this.textInput.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                this.sendMessage();
            }
        });

        this.sendButton = document.createElement("button");
        this.sendButton.className = "chat-send-btn";
        this.sendButton.textContent = "Send";
        this.sendButton.onclick = () => this.sendMessage();

        inputContainer.appendChild(this.textInput);
        inputContainer.appendChild(this.sendButton);
        this.chatContainer.appendChild(inputContainer);

        this.target.appendChild(this.chatContainer);

        // Welcome message (not added to history)
        this.displayMessage("assistant", "Hello! I can help you explore your Power BI data. What would you like to know?");
    }

    private displayMessage(role: 'user' | 'assistant', content: string): void {
        const div = document.createElement("div");
        div.className = `chat-message ${role}`;
        
        const contentDiv = document.createElement("div");
        contentDiv.className = "message-content";
        contentDiv.textContent = content;
        
        div.appendChild(contentDiv);
        this.messagesContainer.appendChild(div);
        this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
    }

    private async sendMessage(): Promise<void> {
        const userMessage = this.textInput.value.trim();
        if (!userMessage) return;

        // Display and add to history
        this.displayMessage("user", userMessage);
        this.history.push({ role: "user", content: userMessage });
        this.textInput.value = "";
        this.sendButton.disabled = true;

        try {
            this.setStatus("Thinking...", "info");

            // Prepare request with history (excluding the current message which is in history)
            const request: ChatRequest = {
                message: userMessage,
                history: this.history.slice(0, -1)  // history before this message
            };

            // Call the API
            const response = await fetch(`${this.serverUrl}/chat`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                body: JSON.stringify(request)
            });

            if (!response.ok) {
                const errText = await response.text();
                throw new Error(`Server error: ${response.status} - ${errText}`);
            }

            const data: ChatResponse = await response.json();
            const reply = data.reply;

            // Add assistant reply to history and display
            this.history.push({ role: "assistant", content: reply });
            this.displayMessage("assistant", reply);
            this.setStatus("Connected", "success");

        } catch (error) {
            const errMsg = (error as Error).message;
            this.displayMessage("assistant", `❌ Error: ${errMsg}`);
            this.setStatus("Error", "error");
        } finally {
            this.sendButton.disabled = false;
        }
    }

    private setStatus(text: string, state: "info" | "success" | "error"): void {
        if (!this.statusBanner) return;
        this.statusBanner.textContent = text;
        this.statusBanner.className = `chat-status ${state}`;
    }

    public update(options: VisualUpdateOptions): void {
        if (options.dataViews && options.dataViews[0]) {
            this.formattingSettings = this.formattingSettingsService.populateFormattingSettingsModel(
                VisualFormattingSettingsModel,
                options.dataViews[0]
            );
        }
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        return this.formattingSettingsService.buildFormattingModel(this.formattingSettings);
    }
}
