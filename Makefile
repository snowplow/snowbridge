.PHONY: all format lint tidy test release release-dry clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

build_dir = build

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

output_dir = $(build_dir)/output
linux_dir  = $(output_dir)/linux

lambda_dir       = $(linux_dir)/lambda
bin_linux_lambda = $(lambda_dir)/main

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all:
	GO111MODULE=on go get -u github.com/mitchellh/gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(bin_linux_lambda) ./cmd/lambda/

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	GO111MODULE=on go fmt ./...
	GO111MODULE=on gofmt -s -w .

lint:
	GO111MODULE=on go get -u golang.org/x/lint/golint
	GO111MODULE=on golint ./...

tidy:
	GO111MODULE=on go mod tidy

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test:
	mkdir -p $(coverage_dir)
	GO111MODULE=on go get -u golang.org/x/tools/cmd/cover
	GO111MODULE=on go test ./... -tags test -v -covermode=count -coverprofile=$(coverage_out)
	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)

# -----------------------------------------------------------------------------
#  RELEASE
# -----------------------------------------------------------------------------

release:
	release-manager --config .release.yml --check-version --make-artifact --make-version --upload-artifact

release-dry:
	release-manager --config .release.yml --check-version --make-artifact

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------

clean:
	rm -rf $(build_dir)
