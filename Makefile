.PHONY: all gox cli cli-linux cli-darwin cli-windows container format lint tidy test-setup test-amd64 test-arm64 integration-reset integration-up integration-down integration-test-amd64 integration-test-arm64 container-release clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

SHELL = /bin/bash
version = `cat VERSION`
aws_only_version = $(version)-aws-only

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

linux_container_image_out_dir = $(output_dir)/container/linux

container_name = snowplow/snowbridge


# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: cli container

gox:
	go install github.com/mitchellh/gox@latest
	mkdir -p $(compiled_dir)

# Build CLI binaries for distro
# First run the commands to compile assets into their 'out_dir' locations
cli: gox cli-linux cli-darwin cli-windows
# Copy the aws licence to current dir for convenience
	cp assets/awslicense/AMAZON_LICENSE AMAZON_LICENSE
# linux aws:
# Zip up the binaries
	(cd $(linux_out_dir)/aws/cli/amd64/ && zip -r staging.zip snowbridge)
# Add the readme, and relevant licence(s)
	zip -u $(linux_out_dir)/aws/cli/amd64/staging.zip README.md LICENSE.md AMAZON_LICENSE
# Move to its compiled_dir location, with appropriate long form name
	mv $(linux_out_dir)/aws/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(aws_only_version)_linux_amd64.zip
# Rinse and repeat for each distribution
# linux arm aws:
	(cd $(linux_out_dir)/aws/cli/arm64/ && zip -r staging.zip snowbridge)
	zip -u $(linux_out_dir)/aws/cli/arm64/staging.zip README.md LICENSE.md AMAZON_LICENSE
	mv $(linux_out_dir)/aws/cli/arm64/staging.zip $(compiled_dir)/snowbridge_$(aws_only_version)_linux_arm64.zip
# darwin aws:
	(cd $(darwin_out_dir)/aws/cli/amd64/ && zip -r staging.zip snowbridge)
	zip -u $(darwin_out_dir)/aws/cli/amd64/staging.zip README.md LICENSE.md AMAZON_LICENSE
	mv $(darwin_out_dir)/aws/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(aws_only_version)_darwin_amd64.zip
# darwin arm aws:
	(cd $(darwin_out_dir)/aws/cli/arm64/ && zip -r staging.zip snowbridge)
	zip -u $(darwin_out_dir)/aws/cli/arm64/staging.zip README.md LICENSE.md AMAZON_LICENSE
	mv $(darwin_out_dir)/aws/cli/arm64/staging.zip $(compiled_dir)/snowbridge_$(aws_only_version)_darwin_arm64.zip
# Windows aws:
	(cd $(windows_out_dir)/aws/cli/amd64/ && zip -r staging.zip snowbridge.exe)
	zip -u $(windows_out_dir)/aws/cli/amd64/staging.zip README.md LICENSE.md AMAZON_LICENSE
	mv $(windows_out_dir)/aws/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(aws_only_version)_windows_amd64.zip
# linux main:
	(cd $(linux_out_dir)/main/cli/amd64/ && zip -r staging.zip snowbridge)
	zip -u $(linux_out_dir)/main/cli/amd64/staging.zip README.md LICENSE.md
	mv $(linux_out_dir)/main/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(version)_linux_amd64.zip
# linux arm main:
	(cd $(linux_out_dir)/main/cli/arm64/ && zip -r staging.zip snowbridge)
	zip -u $(linux_out_dir)/main/cli/arm64/staging.zip README.md LICENSE.md
	mv $(linux_out_dir)/main/cli/arm64/staging.zip $(compiled_dir)/snowbridge_$(version)_linux_arm64.zip
# darwin main:
	(cd $(darwin_out_dir)/main/cli/amd64/ && zip -r staging.zip snowbridge)
	zip -u $(darwin_out_dir)/main/cli/amd64/staging.zip README.md LICENSE.md
	mv $(darwin_out_dir)/main/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(version)_darwin_amd64.zip
# darwin arm main:
	(cd $(darwin_out_dir)/main/cli/arm64/ && zip -r staging.zip snowbridge)
	zip -u $(darwin_out_dir)/main/cli/arm64/staging.zip README.md LICENSE.md
	mv $(darwin_out_dir)/main/cli/arm64/staging.zip $(compiled_dir)/snowbridge_$(version)_darwin_arm64.zip	
# windows main:
	(cd $(windows_out_dir)/main/cli/amd64/ && zip -r staging.zip snowbridge.exe)
	zip -u $(windows_out_dir)/main/cli/amd64/staging.zip README.md LICENSE.md
	mv $(windows_out_dir)/main/cli/amd64/staging.zip $(compiled_dir)/snowbridge_$(version)_windows_amd64.zip

# Build CLI binaries for each distro
cli-linux: gox
	CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/aws/cli/amd64/snowbridge ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/main/cli/amd64/snowbridge ./cmd/main/cli/

	CGO_ENABLED=0 gox -osarch=linux/arm64 -output=$(linux_out_dir)/aws/cli/arm64/snowbridge ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=linux/arm64 -output=$(linux_out_dir)/main/cli/arm64/snowbridge ./cmd/main/cli/	

cli-darwin: gox
	CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/aws/cli/amd64/snowbridge ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/main/cli/amd64/snowbridge ./cmd/main/cli/

	CGO_ENABLED=0 gox -osarch=darwin/arm64 -output=$(darwin_out_dir)/aws/cli/arm64/snowbridge ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=darwin/arm64 -output=$(darwin_out_dir)/main/cli/arm64/snowbridge ./cmd/main/cli/	

cli-windows: gox
	CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/aws/cli/amd64/snowbridge ./cmd/aws/cli/
	CGO_ENABLED=0 gox -osarch=windows/amd64 -output=$(windows_out_dir)/main/cli/amd64/snowbridge ./cmd/main/cli/

container: cli-linux
	docker build -t $(container_name):$(aws_only_version) --platform=linux/amd64 -f Dockerfile.aws .
	docker build -t $(container_name):$(aws_only_version)-arm64 --platform=linux/arm64 -f Dockerfile.aws .
	docker build -t $(container_name):$(version) --platform=linux/amd64 -f Dockerfile.main .
	docker build -t $(container_name):$(version)-arm64 --platform=linux/arm64 -f Dockerfile.main .
	
	docker image inspect $(container_name):$(aws_only_version)
	docker image inspect $(container_name):$(aws_only_version)-arm64
	docker image inspect $(container_name):$(version)
	docker image inspect $(container_name):$(version)-arm64


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
	sudo apt-get update 
	sudo apt install qemu qemu-user-binfmt

test-amd64: test-setup
	GOARCH=amd64 go test $(go_dirs) -v -short -covermode=count -coverprofile=$(coverage_out)
	go tool cover -html=$(coverage_out) -o $(coverage_html)
	go tool cover -func=$(coverage_out)

test-arm64: test-setup
	GOARCH=arm64 go test $(go_dirs) -v -short -covermode=count -coverprofile=$(coverage_out)
	go tool cover -html=$(coverage_out) -o $(coverage_html)
	go tool cover -func=$(coverage_out)	

integration-test-amd64: test-setup
	GOARCH=amd64 go test $(integration_test_dirs) -v -covermode=count -coverprofile=$(coverage_out)
	go tool cover -html=$(coverage_out) -o $(coverage_html)
	go tool cover -func=$(coverage_out)

integration-test-arm64: test-setup
	GOARCH=arm64 go test $(integration_test_dirs) -v -covermode=count -coverprofile=$(coverage_out)
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
	(cd $(integration_dir) && docker compose up -d)
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

# Make & push docker assets, don't tag as latest if there's a `-` in the version (eg. 0.1.0-rc1)
container-release:
	@-docker login --username $(DOCKER_USERNAME) --password $(DOCKER_PASSWORD)
	docker buildx create --name multi-arch-builder --driver=docker-container --platform linux/amd64,linux/arm64 --use
	docker buildx build -t $(container_name):$(aws_only_version) -f Dockerfile.aws --platform=linux/amd64,linux/arm64 --push .
	docker buildx build -t $(container_name):$(version) -f Dockerfile.main --platform=linux/amd64,linux/arm64 --push .

	if ! [[ $(version) =~ "-" ]]; then docker tag ${container_name}:${version} ${container_name}:latest; docker push $(container_name):latest; fi;

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------
clean:
	rm -rf $(build_dir)
	rm -rf $(vendor_dir)
