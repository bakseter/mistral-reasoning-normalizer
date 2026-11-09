package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fake Mistral: emits the user's real chunk-list shape, split awkwardly
// across TCP writes.
func fakeMistral(t *testing.T) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"reasoning_content"`) {
			t.Errorf("reasoning_content leaked upstream: %s", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		events := []string{
			`data: {"id":"a","choices":[{"index":0,"delta":{"content":[{"type":"thinking","thinking":[{"type":"text","text":"The"}],"closed":true}]},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"a","choices":[{"index":0,"delta":{"content":[{"type":"thinking","thinking":[{"type":"text","text":" clusterwide policy:`,
			`- endpointSelector: all"}],"closed":true}]},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"a","choices":[{"index":0,"delta":{"content":[{"type":"text","text":"Here it is"}]},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"a","choices":[{"index":0,"delta":{"content":null,"tool_calls":[{"index":0,"function":{"name":"k8s_get"}}]},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			"data: [DONE]\n\n",
		}
		for _, e := range events {
			io.WriteString(w, e)
			flusher.Flush()
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestProxyEndToEnd(t *testing.T) {
	ts := fakeMistral(t)
	proxy := handler(ts.URL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(
		`{"model":"zai-glm-5-3","stream":true,"messages":[
			{"role":"assistant","content":"partial","reasoning_content":"prior thinking"},
			{"role":"user","content":"continue"}]}`,
	))
	proxy(rec, req)

	// Verify the upstream request was translated (fakeMistral checks the
	// 422-triggering fields are gone; here verify the body reached it).
	got := rec.Body.String()

	for _, want := range []string{
		`"reasoning_content":"The"`,
		"endpointSelector: all",
		`"content":"Here it is"`,
		`"name":"k8s_get"`,
		`"finish_reason":"stop"`,
		"data: [DONE]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in proxied stream:\n%s", want, got)
		}
	}
	if strings.Contains(got, `"type":"thinking"`) {
		t.Errorf("chunk-list JSON leaked through:\n%s", got)
	}
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type lost: %s", ct)
	}
}

// The proxy must translate replayed reasoning into ThinkChunk content.
func TestRequestTranslation(t *testing.T) {
	var seen string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	}))
	defer ts.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(
		`{"model":"zai-glm-5-3","messages":[{"role":"assistant","content":"partial","reasoning_content":"prior"}]}`,
	))
	handler(ts.URL)(rec, req)

	if strings.Contains(seen, `"reasoning_content"`) {
		t.Errorf("reasoning_content forwarded upstream: %s", seen)
	}
	for _, want := range []string{`"type":"thinking"`, `"text":"prior"`, `"text":"partial"`} {
		if !strings.Contains(seen, want) {
			t.Errorf("missing %q in translated request:\n%s", want, seen)
		}
	}
}

// Non-stream JSON must pass through untouched.
func TestNonStreamPassthrough(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":[{"type":"thinking"}]}}]}`)
	}))
	defer ts.Close()

	rec := httptest.NewRecorder()
	handler(ts.URL)(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{}")))
	if !strings.Contains(rec.Body.String(), `"type":"thinking"`) {
		t.Errorf("non-stream body unexpectedly rewritten: %s", rec.Body.String())
	}
}
