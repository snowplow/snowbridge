.PHONY: all gox aws-lambda gcp-cloudfunctions cli cli-linux cli-darwin cli-windows container format lint tidy test-setup test integration-reset integration-up integration-down integration-test container-release clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

go_dirs = `go list ./... | grep -v /build/ | grep -v /vendor/`

build_dir       = build
vendor_dir      = vendor
integration_dir = integration
cert_dir 		= $(integration_dir)/http
abs_cert_dir	= $$(pwd)/$(cert_dir)
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

all: aws-lambda gcp-cloudfunctions cli container

gox:
	GO111MODULE=on go install github.com/mitchellh/gox@latest
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
	cp ./cmd/constants.go $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/cmd/constants.go
	cp ./cmd/init.go $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/cmd/init.go
	cp ./cmd/serverless.go $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/cmd/serverless.go

	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/config/
	cp ./config/config.go $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/config/config.go

	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/
	cp -R ./pkg/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/pkg/

	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/third_party/snowplow/
	cp -R ./third_party/snowplow/badrows/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/third_party/snowplow/badrows
	cp -R ./third_party/snowplow/iglu/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/third_party/snowplow/iglu

	echo "# github.com/snowplow-devops/stream-replicator v$(version)" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/config" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/cmd" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/pkg" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/third_party/snowplow/badrows" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/third_party/snowplow/iglu" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt

	# Create ZIP file for upload to CloudFunctions
	(cd $(staging_dir)/gcp/cloudfunctions/ && zip -r staging.zip .)
	mv $(staging_dir)/gcp/cloudfunctions/staging.zip $(compiled_dir)/gcp_cloudfunctions_stream_replicator_$(version)_linux_amd64.zip

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
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

cli-darwin: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

cli-windows: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/aws/cli/stream-replicator ./cmd/aws/cli/
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/gcp/cli/stream-replicator ./cmd/gcp/cli/

container: cli-linux
	docker build -t $(aws_container_name):$(version) -f Dockerfile.aws .
	docker build -t $(gcp_container_name):$(version) -f Dockerfile.gcp .

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	GO111MODULE=on go fmt $(go_dirs)
	GO111MODULE=on gofmt -s -w .

lint:
	GO111MODULE=on go install golang.org/x/lint/golint@latest
	GO111MODULE=on golint $(go_dirs)

tidy:
	GO111MODULE=on go mod tidy

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test-setup:
	mkdir -p $(coverage_dir)
	GO111MODULE=on go install golang.org/x/tools/cmd/cover@latest

test: test-setup
	GO111MODULE=on go test $(go_dirs) -v -short -covermode=count -coverprofile=$(coverage_out)
	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)
	GO111MODULE=on go tool cover -func=$(coverage_out)

integration-test: test-setup
	export CERT_DIR=$(abs_cert_dir); \
	GO111MODULE=on go test $(go_dirs) -v -covermode=count -coverprofile=$(coverage_out)
	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)
	GO111MODULE=on go tool cover -func=$(coverage_out)

integration-reset: integration-down integration-up

integration-up: http-up
	(cd $(integration_dir) && docker-compose -f ./docker-compose.yml up -d)
	sleep 5

integration-down: http-down
	(cd $(integration_dir) && docker-compose -f ./docker-compose.yml down)
	rm -rf $(integration_dir)/.localstack

http-up:
	(cd "$(integration_dir)/http/server" && go run server.go &)
	sleep 5
	($(ngrok_path) http https://localhost:8999 &>/dev/null &)

http-down:
	(cd "$(integration_dir)/http/shutdown" && go run shutdownRequest.go)
	killall ngrok

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
