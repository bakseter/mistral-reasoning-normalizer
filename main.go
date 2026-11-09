// glm-normalizer-mini: single-file reverse proxy that normalizes Mistral
// reasoning-model output (zai-glm-*, mistral-large-4 with reasoning_effort).
//
// Point agentgateway's backend for these models at this service instead of
// api.mistral.ai. kagent / opencode / open-webui keep calling agentgateway;
// nothing else changes.
//
//   - Streams: delta.content chunk-lists become string content + reasoning_content.
//   - Requests: replayed reasoning_content becomes Mistral ThinkChunk content
//     (Mistral rejects reasoning_content on incoming requests with HTTP 422).
//   - Everything else passes through untouched.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	upstream := getenv("UPSTREAM", "https://api.mistral.ai")
	addr := getenv("ADDR", ":8080")
	log.Printf("mistral-reasoning-normalizer listening on %s -> %s", addr, upstream)
	log.Fatal(http.ListenAndServe(addr, handler(upstream)))
}

func handler(upstream string) http.HandlerFunc {
	client := &http.Client{Transport: http.DefaultTransport}
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		translated := rewriteRequest(body)
		req, err := http.NewRequestWithContext(r.Context(), r.Method,
			upstream+r.URL.Path, bytes.NewReader(translated))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		req.Header = r.Header.Clone()
		req.ContentLength = int64(len(translated))
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			rewriteStream(w.(flushWriter), resp.Body)
		} else {
			io.Copy(w, resp.Body) // non-stream responses pass through
		}
	}
}

type flushWriter interface {
	io.Writer
	http.Flusher
}

// --- streaming rewrite ------------------------------------------------------

func rewriteStream(w flushWriter, body io.Reader) {
	var pending []byte
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		pending = append(pending, buf[:n]...)
		for {
			i := bytes.Index(pending, []byte("\n\n"))
			if i < 0 {
				break
			}
			event := pending[:i+2]
			pending = pending[i+2:]
			w.Write(rewriteEvent(event))
			w.Flush()
		}
		if err != nil {
			break
		}
	}
	w.Write(pending) // partial tail passes through as-is
	w.Flush()
}

func rewriteEvent(event []byte) []byte {
	lines := strings.Split(string(event), "\n")
	for i, line := range lines {
		payload, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "data: ")
		if !ok || payload == "[DONE]" {
			continue
		}
		if out, changed := rewriteChunk([]byte(payload)); changed {
			lines[i] = "data: " + string(out)
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

// --- chunk rewrite ----------------------------------------------------------

func rewriteChunk(payload []byte) ([]byte, bool) {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content json.RawMessage `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(payload, &chunk) != nil || len(chunk.Choices) == 0 {
		return payload, false
	}
	var parts []map[string]any
	if json.Unmarshal(chunk.Choices[0].Delta.Content, &parts) != nil {
		return payload, false // absent, null, or already a string
	}
	var reasoning, text strings.Builder
	hasThinking := false
	for _, p := range parts {
		switch p["type"] {
		case "thinking":
			hasThinking = true
			for _, tAny := range p["thinking"].([]any) {
				t, _ := tAny.(map[string]any)
				if s, _ := t["text"].(string); s != "" {
					reasoning.WriteString(s)
				}
			}
		case "text":
			if s, _ := p["text"].(string); s != "" {
				text.WriteString(s)
			}
		}
	}
	// splice the delta in place, keeping every other field
	var full map[string]any
	json.Unmarshal(payload, &full)
	delta := full["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if hasThinking {
		delta["reasoning_content"] = reasoning.String()
	}
	delta["content"] = text.String()
	out, err := json.Marshal(full)
	if err != nil {
		return payload, false
	}
	return out, true
}

// --- request rewrite --------------------------------------------------------

func rewriteRequest(body []byte) []byte {
	var req map[string]any
	if json.Unmarshal(body, &req) != nil {
		return body
	}
	messages, _ := req["messages"].([]any)
	changed := false
	for _, m := range messages {
		msg, _ := m.(map[string]any)
		if msg == nil || msg["role"] != "assistant" {
			continue
		}
		reasoning, _ := msg["reasoning_content"].(string)
		if reasoning == "" {
			reasoning, _ = msg["reasoning"].(string)
		}
		delete(msg, "reasoning_content") // Mistral 422s on these
		delete(msg, "reasoning")
		if reasoning == "" {
			continue
		}
		if _, native := msg["content"].([]any); native {
			continue // client already speaks Mistral's format
		}
		parts := []any{map[string]any{
			"type":     "thinking",
			"thinking": []any{map[string]any{"type": "text", "text": reasoning}},
		}}
		if s, ok := msg["content"].(string); ok && s != "" {
			parts = append(parts, map[string]any{"type": "text", "text": s})
		}
		msg["content"] = parts
		changed = true
	}
	if !changed {
		return body
	}
	if out, err := json.Marshal(req); err == nil {
		return out
	}
	return body
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
