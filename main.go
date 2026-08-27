package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxRequestBytes        = 4 << 20
	maxDedupEntries        = 100_000
	defaultDeliveryTimeout = 15 * time.Second
)

type Config struct {
	OrgSlug  string    `json:"org_slug"`
	Mappings []Mapping `json:"mappings"`
}

type Mapping struct {
	Application           string `json:"application"`
	Pipeline              string `json:"pipeline"`
	ServiceName           string `json:"service_name"`
	DeploymentEnvironment string `json:"deployment_environment"`
	Env                   string `json:"env,omitempty"`
}

type EchoEvent struct {
	Details struct {
		Source      string `json:"source"`
		Type        string `json:"type"`
		Created     Millis `json:"created"`
		Application string `json:"application"`
	} `json:"details"`
	Content struct {
		ExecutionID string `json:"executionId"`
		Execution   struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			Status    string `json:"status"`
			StartTime int64  `json:"startTime"`
			EndTime   int64  `json:"endTime"`
			Trigger   struct {
				User       string         `json:"user"`
				Parameters map[string]any `json:"parameters"`
				Artifacts  []Artifact     `json:"artifacts"`
			} `json:"trigger"`
			Artifacts []Artifact `json:"artifacts"`
		} `json:"execution"`
	} `json:"content"`
}

type Artifact struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Reference string `json:"reference"`
	Version   string `json:"version"`
}

type Millis int64

func (m *Millis) UnmarshalJSON(data []byte) error {
	text := strings.Trim(strings.TrimSpace(string(data)), `"`)
	if value, err := strconv.ParseInt(text, 10, 64); err == nil {
		*m = Millis(value)
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return fmt.Errorf("timestamp must be epoch milliseconds or RFC3339: %w", err)
	}
	*m = Millis(parsed.UnixMilli())
	return nil
}

type ChangeEvent struct {
	Timestamp  string         `json:"timestamp"`
	EventName  string         `json:"event_name"`
	EventState string         `json:"event_state"`
	Attributes map[string]any `json:"attributes"`
}

type deduper struct {
	mu      sync.Mutex
	entries map[string]time.Time
	ttl     time.Duration
}

func newDeduper(ttl time.Duration) *deduper {
	return &deduper{entries: make(map[string]time.Time), ttl: ttl}
}

func (d *deduper) reserve(key string, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, expires := range d.entries {
		if !expires.After(now) {
			delete(d.entries, k)
		}
	}
	if _, exists := d.entries[key]; exists {
		return false
	}
	if len(d.entries) >= maxDedupEntries {
		var oldestKey string
		var oldestExpiry time.Time
		for candidate, expires := range d.entries {
			if oldestKey == "" || expires.Before(oldestExpiry) {
				oldestKey, oldestExpiry = candidate, expires
			}
		}
		delete(d.entries, oldestKey)
	}
	d.entries[key] = now.Add(d.ttl)
	return true
}

func (d *deduper) release(key string) {
	d.mu.Lock()
	delete(d.entries, key)
	d.mu.Unlock()
}

type tokenSource struct {
	client       *http.Client
	baseURL      string
	accessToken  string
	refreshToken string

	mu        sync.Mutex
	cached    string
	expiresAt time.Time
}

func (s *tokenSource) token(ctx context.Context) (string, error) {
	// Refresh credentials win because access tokens are intentionally short-lived.
	if s.refreshToken == "" && s.accessToken != "" {
		return s.accessToken, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != "" && time.Now().Before(s.expiresAt) {
		return s.cached, nil
	}
	if s.refreshToken == "" {
		return "", errors.New("LAST9_REFRESH_TOKEN is required for long-running use; LAST9_ACCESS_TOKEN is only a local-testing fallback")
	}

	body, _ := json.Marshal(map[string]string{"refresh_token": s.refreshToken})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/api/v4/oauth/access_token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange Last9 refresh token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return "", fmt.Errorf("exchange Last9 refresh token: status %d", resp.StatusCode)
	}
	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("decode Last9 token response: %w", err)
	}
	if result.AccessToken == "" {
		return "", errors.New("Last9 token response omitted access_token")
	}
	ttl := time.Duration(result.ExpiresIn) * time.Second
	if ttl <= time.Minute {
		ttl = 50 * time.Minute
	} else {
		ttl -= time.Minute
	}
	s.cached, s.expiresAt = result.AccessToken, time.Now().Add(ttl)
	return s.cached, nil
}

type last9Client struct {
	httpClient  *http.Client
	baseURL     string
	tokens      *tokenSource
	maxAttempts int
	backoff     time.Duration
	wait        func(context.Context, time.Duration) error
}

func (c *last9Client) send(ctx context.Context, orgSlug string, event ChangeEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	token, err := c.tokens.token(ctx)
	if err != nil {
		return err
	}
	endpoint := c.baseURL + "/api/v4/organizations/" + url.PathEscape(orgSlug) + "/change_events"
	delay := c.backoff
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-LAST9-API-TOKEN", "Bearer "+token)
		resp, err := c.httpClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("Last9 returned status %d", resp.StatusCode)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return lastErr
			}
		} else {
			lastErr = fmt.Errorf("send Last9 change event: %w", err)
		}
		if attempt < c.maxAttempts {
			if err := c.wait(ctx, delay); err != nil {
				return err
			}
			if delay < 15*time.Second {
				delay *= 2
			}
		}
	}
	return lastErr
}

type server struct {
	orgSlug         string
	mappings        map[string]Mapping
	last9           *last9Client
	dedup           *deduper
	eventName       string
	inboundToken    string
	deliveryTimeout time.Duration
	now             func() time.Time
}

func mappingKey(application, pipeline string) string {
	return application + "\x00" + pipeline
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /events", s.handleEvent)
	return mux
}

func (s *server) authorized(r *http.Request) bool {
	if s.inboundToken == "" {
		return true
	}
	want := "Bearer " + s.inboundToken
	got := r.Header.Get("Authorization")
	return len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	var incoming EchoEvent
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&incoming); err != nil {
		http.Error(w, "invalid Echo event", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request must contain one Echo event", http.StatusBadRequest)
		return
	}

	lifecycle, supported := lifecycleFor(incoming.Details.Source, incoming.Details.Type)
	if !supported {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	executionID := incoming.Content.ExecutionID
	if executionID == "" {
		executionID = incoming.Content.Execution.ID
	}
	if incoming.Details.Application == "" || incoming.Content.Execution.Name == "" || executionID == "" {
		http.Error(w, "Echo event is missing application, pipeline, or execution ID", http.StatusBadRequest)
		return
	}
	mapping, ok := s.mappings[mappingKey(incoming.Details.Application, incoming.Content.Execution.Name)]
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	dedupKey := idempotencyKey(executionID, lifecycle)
	if !s.dedup.reserve(dedupKey, s.now()) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	event := buildChangeEvent(incoming, mapping, lifecycle, s.eventName, dedupKey, s.now())
	timeout := s.deliveryTimeout
	if timeout <= 0 {
		timeout = defaultDeliveryTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	if err := s.last9.send(ctx, s.orgSlug, event); err != nil {
		s.dedup.release(dedupKey)
		log.Printf("send change event: %v", err)
		http.Error(w, "Last9 rejected change event", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func lifecycleFor(source, eventType string) (string, bool) {
	if source != "orca" {
		return "", false
	}
	switch eventType {
	case "orca:pipeline:starting":
		return "start", true
	case "orca:pipeline:complete", "orca:pipeline:failed":
		return "stop", true
	default:
		return "", false
	}
}

func idempotencyKey(executionID, lifecycle string) string {
	sum := sha256.Sum256([]byte("spinnaker:" + executionID + ":" + lifecycle))
	return hex.EncodeToString(sum[:])
}

func buildChangeEvent(in EchoEvent, mapping Mapping, lifecycle, eventName, dedupKey string, fallback time.Time) ChangeEvent {
	ts, source := eventTimestamp(in, lifecycle, fallback)
	attributes := map[string]any{
		"service_name":           mapping.ServiceName,
		"deployment_environment": mapping.DeploymentEnvironment,
		"spinnaker_application":  in.Details.Application,
		"spinnaker_pipeline":     in.Content.Execution.Name,
		"spinnaker_execution_id": firstNonEmpty(in.Content.ExecutionID, in.Content.Execution.ID),
		"spinnaker_event_type":   in.Details.Type,
		"spinnaker_status":       in.Content.Execution.Status,
		"outcome":                normalizedOutcome(in.Content.Execution.Status, lifecycle),
		"bridge_source":          "spinnaker",
		"idempotency_key":        dedupKey,
		"timestamp_source":       source,
	}
	if mapping.Env != "" {
		attributes["env"] = mapping.Env
	}
	if user := in.Content.Execution.Trigger.User; user != "" {
		attributes["trigger_user"] = user
	}
	if revision := revisionFrom(in); revision != "" {
		attributes["revision"] = revision
	}
	if image := imageFrom(in); image != "" {
		attributes["image"] = image
	}
	return ChangeEvent{
		Timestamp:  ts.UTC().Format(time.RFC3339Nano),
		EventName:  eventName,
		EventState: lifecycle,
		Attributes: attributes,
	}
}

func eventTimestamp(in EchoEvent, lifecycle string, fallback time.Time) (time.Time, string) {
	if lifecycle == "start" && in.Content.Execution.StartTime > 0 {
		return time.UnixMilli(in.Content.Execution.StartTime), "content.execution.startTime"
	}
	if lifecycle == "stop" && in.Content.Execution.EndTime > 0 {
		return time.UnixMilli(in.Content.Execution.EndTime), "content.execution.endTime"
	}
	if in.Details.Created > 0 {
		return time.UnixMilli(int64(in.Details.Created)), "details.created"
	}
	return fallback, "received_at"
}

func normalizedOutcome(status, lifecycle string) string {
	if lifecycle == "start" {
		return "started"
	}
	switch strings.ToUpper(status) {
	case "SUCCEEDED":
		return "success"
	case "CANCELED":
		return "canceled"
	case "TERMINAL", "FAILED_CONTINUE":
		return "failed"
	case "STOPPED":
		return "stopped"
	case "SKIPPED":
		return "skipped"
	default:
		return "unknown"
	}
}

func revisionFrom(in EchoEvent) string {
	for _, artifacts := range [][]Artifact{in.Content.Execution.Trigger.Artifacts, in.Content.Execution.Artifacts} {
		for _, artifact := range artifacts {
			if value := gitRevision(artifact); value != "" {
				return value
			}
		}
	}
	for _, key := range []string{"revision", "commit", "commit_sha", "version", "image", "tag"} {
		if value, ok := in.Content.Execution.Trigger.Parameters[key]; ok {
			if text, ok := value.(string); ok && text != "" {
				return text
			}
		}
	}
	return ""
}

func gitRevision(artifact Artifact) string {
	if strings.Contains(strings.ToLower(artifact.Type), "git") {
		return firstNonEmpty(artifact.Version, artifact.Reference)
	}
	return ""
}

func imageFrom(in EchoEvent) string {
	for _, artifacts := range [][]Artifact{in.Content.Execution.Trigger.Artifacts, in.Content.Execution.Artifacts} {
		for _, artifact := range artifacts {
			artifactType := strings.ToLower(artifact.Type)
			if strings.Contains(artifactType, "docker") || strings.Contains(artifactType, "image") {
				if value := firstNonEmpty(artifact.Reference, artifact.Name); value != "" {
					return value
				}
			}
		}
	}
	if value, ok := in.Content.Execution.Trigger.Parameters["image"].(string); ok {
		return value
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.OrgSlug == "" {
		return Config{}, errors.New("config must contain org_slug")
	}
	if len(cfg.Mappings) == 0 {
		return Config{}, errors.New("config must contain at least one mapping")
	}
	seen := make(map[string]struct{}, len(cfg.Mappings))
	for i, mapping := range cfg.Mappings {
		if mapping.Application == "" || mapping.Pipeline == "" || mapping.ServiceName == "" || mapping.DeploymentEnvironment == "" {
			return Config{}, fmt.Errorf("mapping %d is missing a required field", i)
		}
		key := mappingKey(mapping.Application, mapping.Pipeline)
		if _, exists := seen[key]; exists {
			return Config{}, fmt.Errorf("duplicate mapping for application %q pipeline %q", mapping.Application, mapping.Pipeline)
		}
		seen[key] = struct{}{}
	}
	return cfg, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func waitFor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func main() {
	configPath := firstNonEmpty(os.Getenv("LAST9_CONFIG_FILE"), "/etc/last9-spinnaker/config.json")
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	baseURL := strings.TrimRight(firstNonEmpty(os.Getenv("LAST9_API_BASE_URL"), "https://app.last9.io"), "/")
	if parsed, err := url.Parse(baseURL); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		log.Fatal("LAST9_API_BASE_URL must be an absolute HTTP(S) URL")
	}
	dedupTTL, err := envDuration("LAST9_DEDUP_TTL", 24*time.Hour)
	if err != nil || dedupTTL <= 0 {
		log.Fatal("LAST9_DEDUP_TTL must be a positive duration")
	}
	backoff, err := envDuration("LAST9_RETRY_BACKOFF", 500*time.Millisecond)
	if err != nil || backoff <= 0 {
		log.Fatal("LAST9_RETRY_BACKOFF must be a positive duration")
	}
	maxAttempts, err := envInt("LAST9_MAX_ATTEMPTS", 4)
	if err != nil || maxAttempts < 1 {
		log.Fatal("LAST9_MAX_ATTEMPTS must be a positive integer")
	}
	deliveryTimeout, err := envDuration("LAST9_DELIVERY_TIMEOUT", defaultDeliveryTimeout)
	if err != nil || deliveryTimeout <= 0 {
		log.Fatal("LAST9_DELIVERY_TIMEOUT must be a positive duration")
	}

	httpClient := &http.Client{Timeout: 15 * time.Second}
	accessToken, refreshToken := os.Getenv("LAST9_ACCESS_TOKEN"), os.Getenv("LAST9_REFRESH_TOKEN")
	if accessToken == "" && refreshToken == "" {
		log.Fatal("LAST9_ACCESS_TOKEN or LAST9_REFRESH_TOKEN is required")
	}
	client := &last9Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		tokens: &tokenSource{
			client:       httpClient,
			baseURL:      baseURL,
			accessToken:  accessToken,
			refreshToken: refreshToken,
		},
		maxAttempts: maxAttempts,
		backoff:     backoff,
		wait:        waitFor,
	}
	mappings := make(map[string]Mapping, len(cfg.Mappings))
	for _, mapping := range cfg.Mappings {
		mappings[mappingKey(mapping.Application, mapping.Pipeline)] = mapping
	}
	handler := (&server{
		orgSlug:         cfg.OrgSlug,
		mappings:        mappings,
		last9:           client,
		dedup:           newDeduper(dedupTTL),
		eventName:       firstNonEmpty(os.Getenv("LAST9_EVENT_NAME"), "deployment"),
		inboundToken:    os.Getenv("SPINNAKER_WEBHOOK_TOKEN"),
		deliveryTimeout: deliveryTimeout,
		now:             time.Now,
	}).routes()
	addr := firstNonEmpty(os.Getenv("LISTEN_ADDR"), ":8080")
	log.Printf("listening on %s with %d explicit mapping(s)", addr, len(mappings))
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      deliveryTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
