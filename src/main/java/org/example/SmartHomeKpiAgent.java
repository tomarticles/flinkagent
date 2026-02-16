package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.annotation.Action;
import org.apache.flink.agents.api.annotation.ChatModelConnection;
import org.apache.flink.agents.api.annotation.ChatModelSetup;
import org.apache.flink.agents.api.annotation.Prompt;
import org.apache.flink.agents.api.annotation.Tool;
import org.apache.flink.agents.api.annotation.ToolParam;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ChatRequestEvent;
import org.apache.flink.agents.api.event.ChatResponseEvent;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceName;

import java.util.List;
import java.util.Map;

public class SmartHomeKpiAgent extends Agent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Strict JSON output prompt (no extra keys)
    private static final org.apache.flink.agents.api.prompt.Prompt KPI_PROMPT =
            org.apache.flink.agents.api.prompt.Prompt.fromText(
                    "You are a smart-home monitoring assistant.\n" +
                            "You analyze ONE sensor snapshot.\n\n" +
                            "Return ONLY valid JSON with EXACTLY these fields:\n" +
                            "  - comfort: one of [\"OK\",\"WARNING\",\"CRITICAL\"]\n" +
                            "  - issues: array of short strings\n" +
                            "  - energyAnomaly: boolean\n" +
                            "  - recommendedActions: array of short strings\n" +
                            "No markdown. No extra keys. No explanations.\n\n" +
                            "Use these rules of thumb:\n" +
                            "- CO2 > 1400 => at least WARNING (ventilation)\n" +
                            "- tempC >= 28 and humidityPct >= 60 => WARNING/CRITICAL (heat stress)\n" +
                            "- powerW >= 3500 => possible energy anomaly unless explicitly expected\n\n" +
                            "Input JSON is in {{input}}."
            );

    @Prompt
    public static org.apache.flink.agents.api.prompt.Prompt kpiPrompt() {
        return KPI_PROMPT;
    }

    @ChatModelConnection
    public static ResourceDescriptor ollamaConnection() {
        return ResourceDescriptor.Builder
                .newBuilder(ResourceName.ChatModel.OLLAMA_CONNECTION)
                .addInitialArgument("endpoint", "http://192.168.1.176:11434")
                .build();
    }

    @ChatModelSetup
    public static ResourceDescriptor kpiModel() {
        return ResourceDescriptor.Builder
                .newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
                .addInitialArgument("connection", "ollamaConnection")
                .addInitialArgument("model", "qwen3:8b")
                .addInitialArgument("prompt", "kpiPrompt")
                .build();
    }

    // Tool: alert ventilation
    @Tool(description = "Raise a ventilation alert for a given home when CO2 is high.")
    public static void ventilationAlert(
            @ToolParam(name = "homeId") String homeId,
            @ToolParam(name = "co2ppm") int co2ppm,
            @ToolParam(name = "message") String message
    ) {
        System.out.printf("[TOOL] ventilationAlert(homeId=%s, co2ppm=%d): %s%n", homeId, co2ppm, message);
    }

    // Tool: alert energy anomaly
    @Tool(description = "Raise an energy alert for a given home when power draw is anomalous.")
    public static void energyAlert(
            @ToolParam(name = "homeId") String homeId,
            @ToolParam(name = "powerW") int powerW,
            @ToolParam(name = "message") String message
    ) {
        System.out.printf("[TOOL] energyAlert(homeId=%s, powerW=%d): %s%n", homeId, powerW, message);
    }

    @Action(listenEvents = {InputEvent.class})
    public static void onInput(InputEvent event, RunnerContext ctx) throws Exception {
        // Input is a JSON string
        String inputJson = (String) event.getInput();
        JsonNode root = MAPPER.readTree(inputJson);

        String homeId = root.path("homeId").asText("unknown");
        int co2ppm = root.path("co2ppm").asInt(-1);
        int powerW = root.path("powerW").asInt(-1);

        // Store what we’ll need later (avoid losing context)
        ctx.getShortTermMemory().set("homeId", homeId);
        ctx.getShortTermMemory().set("co2ppm", co2ppm);
        ctx.getShortTermMemory().set("powerW", powerW);

        // Provide input into the prompt template variable {{input}}
        ChatMessage msg = new ChatMessage(
                MessageRole.USER,
                "",
                Map.of("input", inputJson)
        );

        ctx.sendEvent(new ChatRequestEvent("kpiModel", List.of(msg)));
    }

    @Action(listenEvents = {ChatResponseEvent.class})
    public static void onModelResponse(ChatResponseEvent event, RunnerContext ctx) throws Exception {
        String raw = event.getResponse().getContent();
        JsonNode out = MAPPER.readTree(raw);

        // Validate required fields
        if (out.get("comfort") == null ||
                out.get("issues") == null ||
                out.get("energyAnomaly") == null ||
                out.get("recommendedActions") == null) {
            throw new IllegalStateException("Invalid LLM JSON response: " + raw);
        }

        String homeId = String.valueOf(ctx.getShortTermMemory().get("homeId").getValue());
        int co2ppm = Integer.parseInt(String.valueOf(ctx.getShortTermMemory().get("co2ppm").getValue()));
        int powerW = Integer.parseInt(String.valueOf(ctx.getShortTermMemory().get("powerW").getValue()));

        // Lightweight deterministic triggers (don’t rely only on LLM)
        boolean energyAnomaly = out.path("energyAnomaly").asBoolean(false);

        if (co2ppm >= 1400) {
            ventilationAlert(homeId, co2ppm, "CO2 is high. Recommend airing/ventilation.");
        }
        if (energyAnomaly || powerW >= 3500) {
            energyAlert(homeId, powerW, "Power draw looks high/anomalous. Check appliances (heater/oven/EV charging).");
        }

        // Emit final JSON output (include homeId so downstream can key/route)
        // Keep as String for now, easy to send to Kafka later.
        String outputJson = MAPPER.createObjectNode()
                .put("homeId", homeId)
                .set("analysis", out)
                .toString();

        ctx.sendEvent(new OutputEvent(outputJson));
    }
}