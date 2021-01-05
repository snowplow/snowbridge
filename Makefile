.PHONY: all gox aws-lambda gcp-cloudfunctions cli cli-linux cli-darwin cli-windows format lint tidy test-setup test integration-reset integration-up integration-down integration-test clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

go_dirs = `go list ./... | grep -v /build/ | grep -v /vendor/`

build_dir       = build
vendor_dir      = vendor
integration_dir = integration

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

output_dir   = $(build_dir)/output
staging_dir  = $(build_dir)/staging
compiled_dir = $(build_dir)/compiled

linux_out_dir   = $(output_dir)/linux
darwin_out_dir  = $(output_dir)/darwin
windows_out_dir = $(output_dir)/windows

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: aws-lambda gcp-cloudfunctions cli

gox:
	GO111MODULE=on go get -u github.com/mitchellh/gox
	mkdir -p $(compiled_dir)

aws-lambda: gox
	# WARNING: Binary must be called 'main' to work in Lambda
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/aws/lambda/main ./cmd/aws/lambda/

	# Create ZIP file for upload to Lambda
	(cd $(linux_out_dir)/aws/lambda/ && zip -r staging.zip main)
	mv $(linux_out_dir)/aws/lambda/staging.zip $(compiled_dir)/aws_lambda_stream_replicator_$(version)_linux_amd64.zip

gcp-cloudfunctions: gox
	mkdir -p $(staging_dir)/gcp/cloudfunctions

	# Copy dependencies into staging area
	cp ./cmd/gcp/cloudfunctions/function.go $(staging_dir)/gcp/cloudfunctions/function.go

	# Get module dependencies in a vendor directory
	GO111MODULE=on go mod vendor
	cp -R ./$(vendor_dir)/ $(staging_dir)/gcp/cloudfunctions/vendor/

	# Copy local packages into staging area
	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/cmd/
	cp ./cmd/serverless.go $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/cmd/serverless.go
	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/
	cp -R ./internal/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/internal/
	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/
	cp -R ./pkg/retry/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/retry/
	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/snowplow/
	cp -R ./pkg/snowplow/badrows/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/snowplow/badrows
	cp -R ./pkg/snowplow/iglu/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/snowplow/iglu
	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/third_party/
	cp -R ./third_party/sentryhook/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/third_party/sentryhook/

	echo "# github.com/snowplow-devops/stream-replicator v$(version)" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/cmd" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/internal" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/pkg/retry" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/pkg/snowplow/badrows" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/pkg/snowplow/iglu" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/third_party/sentryhook" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt

	# Create ZIP file for upload to CloudFunctions
	(cd $(staging_dir)/gcp/cloudfunctions/ && zip -r staging.zip .)
	mv $(staging_dir)/gcp/cloudfunctions/staging.zip $(compiled_dir)/gcp_cloudfunctions_stream_replicator_$(version)_linux_amd64.zip

cli: gox cli-linux cli-darwin cli-windows
	(cd $(linux_out_dir)/cli/ && zip -r staging.zip stream-replicator)
	mv $(linux_out_dir)/cli/staging.zip $(compiled_dir)/cli_stream_replicator_$(version)_linux_amd64.zip
	(cd $(darwin_out_dir)/cli/ && zip -r staging.zip stream-replicator)
	mv $(darwin_out_dir)/cli/staging.zip $(compiled_dir)/cli_stream_replicator_$(version)_darwin_amd64.zip
	(cd $(windows_out_dir)/cli/ && zip -r staging.zip stream-replicator.exe)
	mv $(windows_out_dir)/cli/staging.zip $(compiled_dir)/cli_stream_replicator_$(version)_windows_amd64.zip

cli-linux: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/cli/stream-replicator ./cmd/cli/

cli-darwin: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/cli/stream-replicator ./cmd/cli/

cli-windows: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/cli/stream-replicator ./cmd/cli/

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	GO111MODULE=on go fmt $(go_dirs)
	GO111MODULE=on gofmt -s -w .

lint:
	GO111MODULE=on go get -u golang.org/x/lint/golint
	GO111MODULE=on golint $(go_dirs)

tidy:
	GO111MODULE=on go mod tidy

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test-setup:
	mkdir -p $(coverage_dir)
	GO111MODULE=on go get -u golang.org/x/tools/cmd/cover

test: test-setup
	GO111MODULE=on go test $(go_dirs) -v -covermode=count -coverprofile=$(coverage_out)
	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)

integration-test: test-setup
	GO111MODULE=on go test $(go_dirs) -tags integration -v -covermode=count -coverprofile=$(coverage_out)
	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)

integration-reset: integration-down integration-up

integration-up:
	(cd $(integration_dir) && docker-compose -f ./docker-compose.yml up -d)
	sleep 5

integration-down:
	(cd $(integration_dir) && docker-compose -f ./docker-compose.yml down)
	rm -rf $(integration_dir)/.localstack

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------

clean:
	rm -rf $(build_dir)
	rm -rf $(vendor_dir)
