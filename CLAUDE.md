# Snowbridge

Snowbridge is a high-performance data forwarding tool that replicates streams from supported sources to external destinations, with optional filtering and transformation. Written in Go, module path `github.com/snowplow/snowbridge/v5`.

## Architecture

The data flow is: **Source -> Transformation Pipeline -> Router -> Target(s)**

- **Source** (`pkg/source/sourceiface/`): Reads data and pushes `*models.Message` into a channel.
- **Target** (`pkg/target/targetiface/`): Batches and writes messages.
- **Transform** (`pkg/transform/`): Pipeline of transformation functions. Each returns one of: transformed message, filtered message, or failure message.
- **Router** (`cmd/cli/router.go`): Orchestrates flow from transformations to targets. Routes good messages to `Target`, filtered to `FilterTarget`, invalid/oversized to `FailureTarget`.
- **Config** (`config/`): HCL v2-based configuration. Uses a `Pluggable` interface for component registration.

### Pluggable component pattern

All sources, targets, and transforms implement the `Pluggable` interface (`config/component.go`):
- `ProvideDefault() (any, error)` - returns default config struct
- `Create(i any) (any, error)` - creates the component from decoded config

Targets specifically implement `TargetDriver` which includes `GetDefaultConfiguration()`, `GetBatchingConfig()`, `InitFromConfig()`, `Batcher()`, `Write()`, `Open()`, `Close()`.

### Message model

`pkg/models/message.go` - core data struct with `Data`, `OriginalData`, `PartitionKey`, `HTTPHeaders`, timestamps for latency tracking, and `AckFunc`/`NackFunc` callbacks for at-least-once delivery.

## Critical constraints

- **At-least-once semantics**: Messages must be acked only after successful write. Never drop messages silently.
- **Horizontal CPU-based scaling**: Design must support multiple instances.
- **Backpressure**: Batching uses throttle channels and configurable concurrent batch limits.
- **Batching**: Each target implements its own `Batcher()` method on the `TargetDriver` interface. `targetiface.DefaultBatcher` is a shared helper most targets use, but not all (e.g. HTTP). Oversized messages must be routed to failure target, not dropped.
- **OriginalData preservation**: `Message.Data` gets transformed but `OriginalData` must remain untouched — failure handling depends on it to include the original payload.
- **Write failures must be handled gracefully**: Targets must handle write failures without data loss.
- **Panics are only for unrecoverable bugs**: Only panic for unexpected scenarios that require a code change to fix. All other errors must be handled gracefully.
- **Breaking changes require a major version bump**: Avoid breaking changes where possible. Be wary of subtle behaviour changes that may not look breaking but affect customers' use cases.

## Build and test

```bash
make test                  # Unit tests (-short flag, no docker deps)
make lint                  # golangci-lint
make format                # go fmt + gofmt -s

make integration-up        # Start Docker services (LocalStack, Pub/Sub emulator, Kafka)
make integration-test      # Integration tests (requires docker services)
make integration-down      # Stop Docker services

make e2e-up                # Start full e2e environment
make e2e-test              # End-to-end release tests
make e2e-down              # Stop e2e environment

make cli                   # Build multi-platform binaries
make container             # Build Docker images
```

## Configuration

HCL v2 config file, path set via `SNOWBRIDGE_CONFIG_FILE` env var. Key blocks:

```hcl
source {
  use "<name>" { ... }
}
target {
  use "<name>" { ... }
}
failure_target {
  use "<name>" { ... }
}
filter_target {
  use "<name>" { ... }
}
transform {
  use "<name>" { ... }  # multiple allowed, forms pipeline
  worker_pool = 0       # 0 = runtime.GOMAXPROCS(0) + 1
}
retry {
  transient { delay_ms = 1000; max_attempts = 5 }
  setup { delay_ms = 20000; max_attempts = 5 }
  throttle { delay_ms = 10000; max_attempts = 5 }
}
```

Other env vars: `ACCEPT_LIMITED_USE_LICENSE`, `SENTRY_DSN`, `LOG_LEVEL`.

## Two distributions

- **Main** (`Dockerfile.main`): All sources/targets except Kinesis.
- **AWS-only** (`Dockerfile.aws`, build tag `awsonly`): Includes Kinesis source/target.

The `awsonly` build tag controls conditional compilation via paired files (e.g. `source_config_awsonly.go` / `source_config_nonaws.go` in `pkg/source/sourceconfig/`). The non-AWS file uses `//go:build !awsonly` and returns an error for Kinesis; the AWS file uses `//go:build awsonly` and imports the Kinesis source. Tests follow the same pattern with matching `_test.go` files.

## Conventions

- Docs tests (`docs/`): HCL config examples in `assets/docs/configuration/` are used directly in published documentation. Tests in `docs/` verify these examples parse correctly and cover all config fields. Adding a config field requires updating the corresponding example HCL file.
- Linting: golangci-lint with `standard` linters preset (`.golangci.yml`)
- Tests use `testify` (assert/require). Integration tests need Docker via `make integration-up`.
- Branching: feature branches off `develop`, squash-and-merge. Aim for 1 feature = 1 commit.
- Commit messages: imperative case, describe the feature (e.g. `Add claude.md`, `Fix flaky tests`). Reference issues with `(closes #1234)` when applicable.
- Default branch: `develop` (PR target for day-to-day work). `master` is the release branch merged to for production.

## Release process

1. Create a `release/x.y.z` branch from `develop`
2. Add a "prepare for release" commit (`Prepare for X.Y.Z release`) that updates:
   - `VERSION` - new version number
   - `cmd/constants.go` - `AppVersion` constant
   - `CHANGELOG` - new entry at the top
   - `README.md` - version badge
3. PR the release branch to `master`, merge, then tag as `vX.Y.Z`
4. Pre-release tags use suffixes: `vX.Y.Z-rc1`, `vX.Y.Z-test1`, etc.

CHANGELOG format:
```
Version X.Y.Z (YYYY-MM-DD)
--------------------------
One line per change
Another change (#PR)
```
