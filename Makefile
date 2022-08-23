.PHONY: all gox cli cli-linux cli-darwin cli-windows container format lint tidy test-setup test integration-reset integration-up integration-down integration-test container-release clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

go_dirs = `go list ./... | grep -v /build/ | grep -v /vendor/`
integration_test_dirs = `go list ./... | grep -v /build/ | grep -v /vendor/ | grep -v release_test`

build_dir       = build
vendor_dir      = vendor
integration_dir = integration
ngrok_path		= ${NGROK_DIR}ngrok # Set NGROK_DIR to `/path/to/directory/` for local setup

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

output_dir   = $(build_dir)/output
staging_dir  = $(build_dir)/staging
compiled_dir = $(build_dir)/compiled

linux_out_dir   = $(output_dir)/linux
darwin_out_dir  = $(output_dir)/darwin
windows_out_dir = $(output_dir)/windows

aws_container_name = snowplow/stream-replicator-aws
gcp_container_name = snowplow/stream-replicator-gcp

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: cli container

gox:
	go install github.com/mitchellh/gox@latest
	mkdir -p $(compiled_dir)

cli: gox cli-linux cli-darwin cli-windows
	(cd $(linux_out_dir)/aws/cli/ && zip -r staging.zip stream-replicator)
	mv $(linux_out_dir)/aws/cli/staging.zip $(compiled_dir)/aws_cli_stream_replicator_$(version)_linux_amd64.zip
	(cd $(darwin_out_dir)/aws/cli/ && zip -r staging.zip stream-replicator)
	mv $(darwin_out_dir)/aws/cli/staging.zip $(compiled_dir)/aws_cli_stream_replicator_$(version)_darwin_amd64.zip
	(cd $(windows_out_dir)/aws/cli/ && zip -r staging.zip stream-replicator.exe)
	mv $(windows_out_dir)/aws/cli/staging.zip $(compiled_dir)/aws_cli_stream_replicator_$(version)_windows_amd64.zip
	(cd $(linux_out_dir)/gcp/cli/ && zip -r staging.zip stream-replicator)
	mv $(linux_out_dir)/gcp/cli/staging.zip $(compiled_dir)/gcp_cli_stream_replicator_$(version)_linux_amd64.zip
	(cd $(darwin_out_dir)/gcp/cli/ && zip -r staging.zip stream-replicator)
	mv $(darwin_out_dir)/gcp/cli/staging.zip $(compiled_dir)/gcp_cli_stream_replicator_$(version)_darwin_amd64.zip
	(cd $(windows_out_dir)/gcp/cli/ && zip -r staging.zip stream-replicator.exe)
	mv $(windows_out_dir)/gcp/cli/staging.zip $(compiled_dir)/gcp_cli_stream_replicator_$(version)_windows_amd64.zip

cli-linux: gox
	CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

cli-darwin: gox
	CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

cli-windows: gox
	CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

container: cli-linux
	docker build -t $(aws_container_name):$(version) -f Dockerfile.aws .
	docker build -t $(gcp_container_name):$(version) -f Dockerfile.gcp .

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	go fmt $(go_dirs)
	gofmt -s -w .

lint:
	go install golang.org/x/lint/golint@latest
	LINTRESULT=$$(golint $(go_dirs)); echo "$$LINTRESULT"; [ -z "$$LINTRESULT" ];

tidy:
	go mod tidy

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test-setup:
	mkdir -p $(coverage_dir)
	go install golang.org/x/tools/cmd/cover@latest

test: test-setup
	go test $(go_dirs) -v -short -covermode=count -coverprofile=$(coverage_out)
	go tool cover -html=$(coverage_out) -o $(coverage_html)
	go tool cover -func=$(coverage_out)

integration-test: test-setup
	go test $(integration_test_dirs) -v -covermode=count -coverprofile=$(coverage_out)
	go tool cover -html=$(coverage_out) -o $(coverage_html)
	go tool cover -func=$(coverage_out)

# e2e-test covers only the e2e release tests, in preparation for when these will rely on deployed assets
e2e-test: test-setup
	go test ./release_test -v


e2e-reset: e2e-down e2e-up

e2e-up:
	(cd $(integration_dir) && docker compose up -d)
	sleep 5

e2e-down: 
	(cd $(integration_dir) && docker compose down)
	rm -rf $(integration_dir)/.localstack

integration-reset: integration-down integration-up

# For integration tests we need localstack and pubsub, but not kafka (yet)
integration-up: http-up
	(cd $(integration_dir) && docker compose up -d localstack pubsub)
	sleep 5

# We can just shut everything down here
integration-down: http-down
	(cd $(integration_dir) && docker compose down)
	rm -rf $(integration_dir)/.localstack

# ngrok needs to be installed and auth token must be configured for this if running locally
http-up:
	(cd "$(integration_dir)/http/server" && go run server.go &)
	sleep 5
	($(ngrok_path) http https://localhost:8999 &>/dev/null &)

http-down:
	(cd "$(integration_dir)/http/shutdown" && go run shutdownRequest.go)
	killall ngrok || true 

# -----------------------------------------------------------------------------
#  RELEASE
# -----------------------------------------------------------------------------

container-release:
	@-docker login --username $(DOCKER_USERNAME) --password $(DOCKER_PASSWORD)
	docker push $(aws_container_name):$(version)
	docker tag ${aws_container_name}:${version} ${aws_container_name}:latest
	docker push $(aws_container_name):latest
	docker push $(gcp_container_name):$(version)
	docker tag ${gcp_container_name}:${version} ${gcp_container_name}:latest
	docker push $(gcp_container_name):latest

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------

clean:
	rm -rf $(build_dir)
	rm -rf $(vendor_dir)
