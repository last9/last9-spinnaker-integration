package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestLast9ClientRefreshesOnceAfterAuthorizationFailure(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var tokenCalls, eventCalls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v4/oauth/access_token" {
					call := tokenCalls.Add(1)
					_, _ = io.WriteString(w, `{"access_token":"access-`+strconv.Itoa(int(call))+`","expires_in":3600}`)
					return
				}
				call := eventCalls.Add(1)
				if call == 1 {
					if got := r.Header.Get("X-LAST9-API-TOKEN"); got != "Bearer access-1" {
						t.Fatalf("first authorization = %q", got)
					}
					w.WriteHeader(status)
					return
				}
				if got := r.Header.Get("X-LAST9-API-TOKEN"); got != "Bearer access-2" {
					t.Fatalf("refreshed authorization = %q", got)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer api.Close()
			client := &last9Client{
				httpClient: api.Client(), baseURL: api.URL,
				tokens:      &tokenSource{client: api.Client(), baseURL: api.URL, refreshToken: "refresh"},
				maxAttempts: 1, backoff: time.Millisecond,
				wait: func(context.Context, time.Duration) error { return nil },
			}
			if err := client.send(context.Background(), "example", ChangeEvent{EventName: "deployment"}); err != nil {
				t.Fatal(err)
			}
			if tokenCalls.Load() != 2 || eventCalls.Load() != 2 {
				t.Fatalf("token calls=%d event calls=%d, want 2 each", tokenCalls.Load(), eventCalls.Load())
			}
		})
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

func TestHandlerDoesNotAcknowledgeInFlightDuplicate(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	m := mapping()
	s := &server{
		orgSlug: "example", mappings: map[string]Mapping{mappingKey(m.Application, m.Pipeline): m},
		last9: &last9Client{
			httpClient: api.Client(), baseURL: api.URL,
			tokens: &tokenSource{accessToken: "token"}, maxAttempts: 1,
			backoff: time.Millisecond, wait: func(context.Context, time.Duration) error { return nil },
		},
		dedup: newDeduper(time.Hour), eventName: "deployment", deliveryTimeout: time.Second,
		now: func() time.Time { return time.Unix(1_800_000_000, 0) },
	}
	body, _ := json.Marshal(fixture("orca:pipeline:starting", "RUNNING"))
	send := func() int {
		req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(string(body)))
		resp := httptest.NewRecorder()
		s.routes().ServeHTTP(resp, req)
		return resp.Code
	}
	leaderDone := make(chan int, 1)
	go func() { leaderDone <- send() }()
	<-started
	if got := send(); got != http.StatusServiceUnavailable {
		t.Fatalf("in-flight duplicate status = %d, want %d", got, http.StatusServiceUnavailable)
	}
	close(release)
	if got := <-leaderDone; got != http.StatusBadGateway {
		t.Fatalf("failed leader status = %d, want %d", got, http.StatusBadGateway)
	}
	if got := send(); got != http.StatusAccepted {
		t.Fatalf("retry after leader failure status = %d, want %d", got, http.StatusAccepted)
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
		{"starting", `{"details":{"source":"orca","type":"orca:pipeline:starting","created":"1700000000000","application":"payments"},"content":{"executionId":"execution-start","execution":{"id":"execution-start","name":"deploy-production","status":"RUNNING","startTime":1700000001000,"trigger":{"type":"manual","user":"operator@example.com","parameters":{},"artifacts":[{"type":"docker/image","name":"registry.example/payments","reference":"registry.example/payments@sha256:def456","version":"sha256:def456"},{"type":"git/repo","name":"repository","reference":"abc123","version":"abc123"},{"type":"s3/object","name":"s3://example-bucket/release,part.tgz","reference":"s3://example-bucket/first","version":"v7"},{"type":"s3/object","name":"s3://example-bucket/release,part.tgz","reference":"s3://example-bucket/second","version":"v7"}]}}}}`, "start", "started"},
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
			if event.Attributes["trigger_type"] != "manual" {
				t.Fatalf("trigger type not extracted: %#v", event.Attributes)
			}
			if test.name == "starting" {
				if event.Attributes["artifact_count"] != "4" {
					t.Fatalf("artifact count = %q, want 4", event.Attributes["artifact_count"])
				}
				var artifacts []redactedArtifact
				if err := json.Unmarshal([]byte(event.Attributes["artifacts"]), &artifacts); err != nil {
					t.Fatal(err)
				}
				if len(artifacts) != 4 {
					t.Fatalf("artifacts = %#v, want 4 tuples", artifacts)
				}
				counts := make(map[string]int)
				s3Hashes := make(map[string]struct{})
				for _, artifact := range artifacts {
					if len(artifact.ReferenceSHA256) != sha256.Size*2 {
						t.Fatalf("reference hash is not fixed length: %#v", artifact)
					}
					key := artifact.Type + "\x00" + artifact.Name + "\x00" + artifact.Version
					counts[key]++
					if artifact.Type == "s3/object" {
						s3Hashes[artifact.ReferenceSHA256] = struct{}{}
					}
				}
				if counts["docker/image\x00registry.example/payments\x00sha256:def456"] != 1 || counts["git/repo\x00repository\x00abc123"] != 1 || counts["s3/object\x00s3://example-bucket/release,part.tgz\x00v7"] != 2 || len(s3Hashes) != 2 {
					t.Fatalf("artifact associations/reference identity lost: %#v", artifacts)
				}
				if strings.Contains(event.Attributes["artifacts"], "s3://example-bucket/first") || strings.Contains(event.Attributes["artifacts"], "s3://example-bucket/second") {
					t.Fatalf("artifact references leaked: %s", event.Attributes["artifacts"])
				}
			}
		})
	}
}

func TestArtifactMetadataIsBounded(t *testing.T) {
	in := fixture("orca:pipeline:starting", "RUNNING")
	in.Content.Execution.Trigger.Artifacts = append(in.Content.Execution.Trigger.Artifacts,
		Artifact{Type: "00-long", Name: strings.Repeat("x", maxArtifactFieldBytes+10), Version: "v1"},
		Artifact{Reference: "hidden-only-reference"},
	)
	for i := 0; i < maxArtifactCount+2; i++ {
		in.Content.Execution.Trigger.Artifacts = append(in.Content.Execution.Trigger.Artifacts, Artifact{
			Type: "kind-" + strconv.Itoa(i), Name: "artifact-" + strconv.Itoa(i), Version: "v1",
		})
	}
	event := buildChangeEvent(in, mapping(), "start", "deployment", "key", time.Unix(0, 0))
	if event.Attributes["artifact_count"] != strconv.Itoa(maxArtifactCount) || event.Attributes["artifact_metadata_truncated"] != "true" {
		t.Fatalf("unexpected bounded metadata: %#v", event.Attributes)
	}
	var artifacts []redactedArtifact
	if err := json.Unmarshal([]byte(event.Attributes["artifacts"]), &artifacts); err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != maxArtifactCount {
		t.Fatalf("artifact count = %d, want %d", len(artifacts), maxArtifactCount)
	}
	longFieldWasBounded := false
	for _, artifact := range artifacts {
		if artifact.Type == "00-long" && len(artifact.Name) == maxArtifactFieldBytes {
			longFieldWasBounded = true
		}
	}
	if !longFieldWasBounded || strings.Contains(event.Attributes["artifacts"], "hidden-only-reference") {
		t.Fatalf("artifact bounds/redaction failed: %s", event.Attributes["artifacts"])
	}
}

func TestRefreshTokenTakesPrecedence(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"refresh_token":"refresh"`) {
			t.Fatalf("unexpected body: %s", body)
		}
		_, _ = io.WriteString(w, `{"access_token":"access","expires_in":3600}`)
	}))
	defer api.Close()
	tokens := &tokenSource{client: api.Client(), baseURL: api.URL, accessToken: "fallback", refreshToken: "refresh"}
	token, err := tokens.token(context.Background())
	if err != nil || token != "access" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("token exchange calls = %d, want 1", calls.Load())
	}
}

func TestShortLivedTokenCachesForHalfItsLifetime(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls.Add(1)
		_, _ = io.WriteString(w, `{"access_token":"access-`+strconv.Itoa(int(call))+`","expires_in":30}`)
	}))
	defer api.Close()
	now := time.Unix(1_800_000_000, 0)
	tokens := &tokenSource{
		client: api.Client(), baseURL: api.URL, refreshToken: "refresh",
		now: func() time.Time { return now },
	}

	first, err := tokens.token(context.Background())
	if err != nil || first != "access-1" {
		t.Fatalf("first token=%q err=%v", first, err)
	}
	if want := now.Add(15 * time.Second); !tokens.expiresAt.Equal(want) {
		t.Fatalf("cache expiry = %s, want %s", tokens.expiresAt, want)
	}
	now = now.Add(14 * time.Second)
	second, err := tokens.token(context.Background())
	if err != nil || second != "access-1" || calls.Load() != 1 {
		t.Fatalf("cached token=%q calls=%d err=%v", second, calls.Load(), err)
	}
	now = now.Add(2 * time.Second)
	third, err := tokens.token(context.Background())
	if err != nil || third != "access-2" || calls.Load() != 2 {
		t.Fatalf("refreshed token=%q calls=%d err=%v", third, calls.Load(), err)
	}
}
