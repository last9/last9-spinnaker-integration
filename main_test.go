package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(eventType, status string) EchoEvent {
	var event EchoEvent
	event.Details.Source = "orca"
	event.Details.Type = eventType
	event.Details.Created = Millis(1_700_000_000_000)
	event.Details.Application = "payments"
	event.Content.ExecutionID = "execution-123"
	event.Content.Execution.ID = "execution-123"
	event.Content.Execution.Name = "deploy-production"
	event.Content.Execution.Status = status
	event.Content.Execution.StartTime = 1_700_000_001_000
	event.Content.Execution.EndTime = 1_700_000_061_000
	event.Content.Execution.Trigger.User = "operator@example.com"
	event.Content.Execution.Trigger.Parameters = map[string]any{"revision": "abc123"}
	return event
}

func mapping() Mapping {
	return Mapping{
		Application:           "payments",
		Pipeline:              "deploy-production",
		ServiceName:           "payments-api",
		DeploymentEnvironment: "production",
		Env:                   "production",
	}
}

func TestBuildChangeEventLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		typeName  string
		status    string
		lifecycle string
		outcome   string
		timestamp string
	}{
		{"start", "orca:pipeline:starting", "RUNNING", "start", "started", "2023-11-14T22:13:21Z"},
		{"success", "orca:pipeline:complete", "SUCCEEDED", "stop", "success", "2023-11-14T22:14:21Z"},
		{"failure", "orca:pipeline:failed", "TERMINAL", "stop", "failed", "2023-11-14T22:14:21Z"},
		{"cancel", "orca:pipeline:complete", "CANCELED", "stop", "canceled", "2023-11-14T22:14:21Z"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := buildChangeEvent(fixture(test.typeName, test.status), mapping(), test.lifecycle, "deployment", "key", time.Unix(0, 0))
			if event.EventState != test.lifecycle || event.Attributes["outcome"] != test.outcome || event.Timestamp != test.timestamp {
				t.Fatalf("unexpected event: %#v", event)
			}
			if _, exists := event.Attributes["data_source_name"]; exists {
				t.Fatal("data_source_name must not be emitted")
			}
			if event.Attributes["service_name"] != "payments-api" || event.Attributes["deployment_environment"] != "production" {
				t.Fatalf("missing correlation attributes: %#v", event.Attributes)
			}
		})
	}
}

func TestEchoCreatedAcceptsQuotedMilliseconds(t *testing.T) {
	var event EchoEvent
	if err := json.Unmarshal([]byte(`{"details":{"created":"1700000000000"}}`), &event); err != nil {
		t.Fatal(err)
	}
	if event.Details.Created != Millis(1_700_000_000_000) {
		t.Fatalf("created = %d", event.Details.Created)
	}
}

func TestLast9ClientRetries(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-LAST9-API-TOKEN"); got != "Bearer token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	client := &last9Client{
		httpClient:  api.Client(),
		baseURL:     api.URL,
		tokens:      &tokenSource{accessToken: "token"},
		maxAttempts: 3,
		backoff:     time.Millisecond,
		wait:        func(context.Context, time.Duration) error { return nil },
	}
	if err := client.send(context.Background(), "example", ChangeEvent{EventName: "deployment"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestHandlerFiltersAndDeduplicates(t *testing.T) {
	var received atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		var event ChangeEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	m := mapping()
	s := &server{
		orgSlug:  "example",
		mappings: map[string]Mapping{mappingKey(m.Application, m.Pipeline): m},
		last9: &last9Client{
			httpClient: api.Client(), baseURL: api.URL,
			tokens: &tokenSource{accessToken: "token"}, maxAttempts: 1,
			backoff: time.Millisecond, wait: func(context.Context, time.Duration) error { return nil },
		},
		dedup:     newDeduper(time.Hour),
		eventName: "deployment",
		now:       func() time.Time { return time.Unix(1_800_000_000, 0) },
	}
	handler := s.routes()

	send := func(event EchoEvent) int {
		body, _ := json.Marshal(event)
		req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(string(body)))
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		return resp.Code
	}
	if got := send(fixture("orca:pipeline:starting", "RUNNING")); got != http.StatusAccepted {
		t.Fatalf("first status = %d", got)
	}
	if got := send(fixture("orca:pipeline:starting", "RUNNING")); got != http.StatusNoContent {
		t.Fatalf("duplicate status = %d", got)
	}
	unmapped := fixture("orca:pipeline:starting", "RUNNING")
	unmapped.Details.Application = "unknown"
	if got := send(unmapped); got != http.StatusNoContent {
		t.Fatalf("unmapped status = %d", got)
	}
	unsupported := fixture("orca:stage:complete", "SUCCEEDED")
	if got := send(unsupported); got != http.StatusNoContent {
		t.Fatalf("unsupported status = %d", got)
	}
	if received.Load() != 1 {
		t.Fatalf("received %d events, want 1", received.Load())
	}
}

func TestHandlerAcceptsEchoLifecyclePayloads(t *testing.T) {
	received := make(chan ChangeEvent, 3)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event ChangeEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Fatal(err)
		}
		received <- event
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	m := mapping()
	s := &server{
		orgSlug:  "example",
		mappings: map[string]Mapping{mappingKey(m.Application, m.Pipeline): m},
		last9: &last9Client{
			httpClient: api.Client(), baseURL: api.URL,
			tokens: &tokenSource{accessToken: "token"}, maxAttempts: 1,
			backoff: time.Millisecond, wait: func(context.Context, time.Duration) error { return nil },
		},
		dedup: newDeduper(time.Hour), eventName: "deployment",
		deliveryTimeout: time.Second,
		now:             func() time.Time { return time.Unix(1_800_000_000, 0) },
	}
	tests := []struct {
		name, payload, state, outcome string
	}{
		{"starting", `{"details":{"source":"orca","type":"orca:pipeline:starting","created":"1700000000000","application":"payments"},"content":{"executionId":"execution-start","execution":{"id":"execution-start","name":"deploy-production","status":"RUNNING","startTime":1700000001000,"trigger":{"type":"manual","user":"operator@example.com","parameters":{},"artifacts":[{"type":"docker/image","name":"registry.example/payments","reference":"registry.example/payments@sha256:def456","version":"sha256:def456"},{"type":"git/repo","name":"repository","reference":"abc123","version":"abc123"}]}}}}`, "start", "started"},
		{"complete", `{"details":{"source":"orca","type":"orca:pipeline:complete","created":"1700000060000","application":"payments"},"content":{"executionId":"execution-complete","execution":{"id":"execution-complete","name":"deploy-production","status":"SUCCEEDED","startTime":1700000001000,"endTime":1700000061000,"trigger":{"type":"manual","user":"operator@example.com","parameters":{},"artifacts":[{"type":"git/repo","name":"repository","reference":"abc123","version":"abc123"},{"type":"docker/image","name":"registry.example/payments","reference":"registry.example/payments@sha256:def456"}]}}}}`, "stop", "success"},
		{"failed", `{"details":{"source":"orca","type":"orca:pipeline:failed","created":"1700000060000","application":"payments"},"content":{"executionId":"execution-failed","execution":{"id":"execution-failed","name":"deploy-production","status":"TERMINAL","startTime":1700000001000,"endTime":1700000061000,"trigger":{"type":"manual","user":"operator@example.com","parameters":{},"artifacts":[{"type":"git/repo","name":"repository","reference":"abc123","version":"abc123"},{"type":"docker/image","name":"registry.example/payments","reference":"registry.example/payments@sha256:def456"}]}}}}`, "stop", "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(test.payload))
			resp := httptest.NewRecorder()
			s.routes().ServeHTTP(resp, req)
			if resp.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
			}
			event := <-received
			if event.EventState != test.state || event.Attributes["outcome"] != test.outcome {
				t.Fatalf("unexpected lifecycle event: %#v", event)
			}
			if event.Attributes["revision"] != "abc123" || event.Attributes["image"] != "registry.example/payments@sha256:def456" {
				t.Fatalf("trigger artifacts not extracted: %#v", event.Attributes)
			}
		})
	}
}

func TestTokenExchange(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"refresh_token":"refresh"`) {
			t.Fatalf("unexpected body: %s", body)
		}
		_, _ = io.WriteString(w, `{"access_token":"access","expires_in":3600}`)
	}))
	defer api.Close()
	tokens := &tokenSource{client: api.Client(), baseURL: api.URL, refreshToken: "refresh"}
	token, err := tokens.token(context.Background())
	if err != nil || token != "access" {
		t.Fatalf("token=%q err=%v", token, err)
	}
}
