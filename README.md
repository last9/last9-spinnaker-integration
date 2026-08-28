# Last9 Spinnaker integration

Forward authoritative Spinnaker pipeline lifecycle events from Echo to the
[Last9 Change Events API](https://last9.io/docs/change-events/).

The bridge accepts Echo's `wrap: false` envelope, ignores non-pipeline events,
maps approved application/pipeline pairs to exact Last9 service and environment
labels, and emits paired `start` and `stop` deployment events.

## Configure

Copy `config.example.json` and add one explicit mapping per production pipeline.
Mappings are exact; the bridge does not infer production from names. Last9
credentials are organization-scoped, so run one bridge deployment and mapping
file per Last9 organization.

| Variable | Required | Default |
| --- | --- | --- |
| `LAST9_CONFIG_FILE` | No | `/etc/last9-spinnaker/config.json` |
| `LAST9_REFRESH_TOKEN` | One token form | — |
| `LAST9_ACCESS_TOKEN` | One token form | — |
| `LAST9_API_BASE_URL` | No | `https://app.last9.io` |
| `LAST9_EVENT_NAME` | No | `deployment` |
| `LAST9_MAX_ATTEMPTS` | No | `4` |
| `LAST9_RETRY_BACKOFF` | No | `500ms` |
| `LAST9_DELIVERY_TIMEOUT` | No | `15s` |
| `LAST9_DEDUP_TTL` | No | `24h` |
| `SPINNAKER_WEBHOOK_TOKEN` | No | — |
| `LISTEN_ADDR` | No | `:8080` |

Use `LAST9_REFRESH_TOKEN` for a long-running deployment. `LAST9_ACCESS_TOKEN`
is a short-lived fallback for local testing; when both are set, the refresh
token takes precedence.

`SPINNAKER_WEBHOOK_TOKEN` enables a bearer-token check on `/events`. Echo's
documented REST listener does not guarantee custom request headers, so otherwise
expose the bridge only through a private endpoint or authenticated gateway.

## Run

```sh
docker build -t last9-spinnaker-integration .
docker run --rm -p 8080:8080 \
  -v "$PWD/config.json:/etc/last9-spinnaker/config.json:ro" \
  -e LAST9_REFRESH_TOKEN \
  last9-spinnaker-integration
```

Configure Echo:

```yaml
rest:
  enabled: true
  endpoints:
    - wrap: false
      url: http://last9-spinnaker-integration:8080/events
```

Echo sends all Orca, Igor, and Echo events to every endpoint. This bridge only
accepts `orca:pipeline:starting`, `orca:pipeline:complete`, and
`orca:pipeline:failed`; unmapped and unrelated events return `204`.

## Behavior

- Terminal outcome comes from `content.execution.status`, preserving cancellation
  and stopped states instead of inferring them from the Echo event name.
- Revision and image attributes come from `content.execution.trigger.artifacts`,
  with trigger parameters and legacy execution artifacts used as fallbacks.
- `trigger_type` comes from `content.execution.trigger.type`. Generic artifact
  metadata is emitted as a deterministic JSON `artifacts` attribute containing
  sorted unique `{type,name,version,reference_sha256}` tuples plus a string
  `artifact_count`. The fixed 64-character reference hash distinguishes
  reference-only artifacts without exposing their raw reference.
  Metadata is capped at 20 artifacts and 256 bytes per field;
  `artifact_metadata_truncated="true"` reports clipping or dropping. Artifact
  references and the rest of the Echo payload are not copied; the existing
  `revision` and `image` attributes retain their specialized behavior.
- `service_name` and `deployment_environment` come only from explicit mappings.
- `data_source_name` is never sent.
- Delivery is at-least-once with best-effort deduplication by execution ID and
  lifecycle. Completed duplicates return `204`; concurrent in-flight duplicates
  return `503` so Echo can retry. The cache is bounded by `LAST9_DEDUP_TTL` but is
  pod-local and resets on restart. Run one replica unless the Last9 API provides
  server-side idempotency for your account. An ambiguous transport failure can
  still duplicate an event if Last9 accepted it before the connection failed.
- A Last9 `401` or `403` invalidates a cached refresh-token credential and gets
  one immediate refresh/retry. Other retries follow `LAST9_MAX_ATTEMPTS`.
- The bridge returns `202` only after Last9 accepts the event. A Last9 failure
  returns `502`, allowing Echo to retry. Delivery is bounded by
  `LAST9_DELIVERY_TIMEOUT`; configure Echo's read timeout above that value.

Before production rollout, capture and redact one starting, successful, failed,
and canceled event from your Spinnaker version. Validate its field paths and each
mapping against the service and environment labels already present in Last9.

## Verify and roll back

Check `GET /healthz`, trigger one approved deployment, and confirm its start and
stop markers have the expected service, environment, execution ID, revision,
user, and outcome. Roll back by removing the Echo endpoint; the bridge does not
modify pipelines or deployed workloads.

## Develop

Go 1.27 is required.

```sh
gofmt -w main.go main_test.go
go test ./...
go vet ./...
go build ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
