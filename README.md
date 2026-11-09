# mistral-reasoning-normalizer

Simple reverse proxy that fixes Mistral reasoning models (`zai-glm-*`, `mistral-large-4` + `reasoning_effort`) for
clients/harnesses that mainly support Open AI format. Deployed as a backend behind agentgateway.

```
kagent / opencode / open-webui → agentgateway → glm-normalizer-mini → api.mistral.ai
```

agentgateway still fronts everything (auth, routing, telemetry, cost
catalog): it just forwards the `zai-glm-*` model's traffic to this service
instead of directly to Mistral. The proxy is transparent — it clones headers
(including the provider key agentgateway injects) and only rewrites:

1. **SSE streams**: `delta.content` chunk-lists → string `content` +
   `reasoning_content`, event-by-event with flushes, robust to events
   split across TCP chunks.
2. **Request bodies**: replayed `reasoning_content`/`reasoning` →
   Mistral ThinkChunk content arrays (Mistral 422s on `reasoning_content`
   in incoming requests), stripped when empty.

Non-stream JSON responses pass through untouched (all three clients
stream by default). If a non-streaming call ever surfaces chunk-lists,
run it through `stream: true` or port `rewriteResponseJSON` from the full
version.
