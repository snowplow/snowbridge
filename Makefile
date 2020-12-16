.PHONY: all gox aws-lambda gcp-cloudfunctions stdin format lint tidy test release release-dry clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

go_dirs = `go list ./... | grep -v /build/ | grep -v /vendor/`

build_dir  = build
vendor_dir = vendor

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

output_dir  = $(build_dir)/output
staging_dir = $(build_dir)/staging

linux_out_dir  = $(output_dir)/linux
darwin_out_dir = $(output_dir)/darwin
zip_out_dir    = $(output_dir)/zip

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: aws-lambda gcp-cloudfunctions stdin

gox:
	GO111MODULE=on go get -u github.com/mitchellh/gox

aws-lambda: gox
	# WARNING: Binary must be called 'main' to work in Lambda
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/aws/lambda/main ./cmd/aws/lambda/

	# Create ZIP file for upload to Lambda
	mkdir -p $(zip_out_dir)/aws/lambda
	(cd $(linux_out_dir)/aws/lambda/ && zip -r staging.zip main)
	mv $(linux_out_dir)/aws/lambda/staging.zip $(zip_out_dir)/aws/lambda/stream_replicator_$(version)_linux_amd64.zip

gcp-cloudfunctions:
	mkdir -p $(staging_dir)/gcp/cloudfunctions

	# Copy dependencies into staging area
	cp ./cmd/gcp/cloudfunctions/function.go $(staging_dir)/gcp/cloudfunctions/function.go

	# Get module dependencies in a vendor directory
	GO111MODULE=on go mod vendor
	cp -R ./$(vendor_dir)/ $(staging_dir)/gcp/cloudfunctions/vendor/

	mkdir -p $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/core/
	cp -R ./core/ $(staging_dir)/gcp/cloudfunctions/vendor/github.com/snowplow-devops/stream-replicator/core/

	echo "# github.com/snowplow-devops/stream-replicator v$(version)" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt
	echo "github.com/snowplow-devops/stream-replicator/core" >> $(staging_dir)/gcp/cloudfunctions/vendor/modules.txt

	# Create ZIP file for upload to CloudFunctions
	mkdir -p $(zip_out_dir)/gcp/cloudfunctions
	(cd $(staging_dir)/gcp/cloudfunctions/ && zip -r staging.zip .)
	mv $(staging_dir)/gcp/cloudfunctions/staging.zip $(zip_out_dir)/gcp/cloudfunctions/stream_replicator_$(version)_linux_amd64.zip

stdin: gox
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=linux/amd64 -output=$(linux_out_dir)/stdin/stream-replicator-$(version) ./cmd/stdin/
	GO111MODULE=on CGO_ENABLED=0 gox -osarch=darwin/amd64 -output=$(darwin_out_dir)/stdin/stream-replicator-$(version) ./cmd/stdin/

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

test:
	mkdir -p $(coverage_dir)
	GO111MODULE=on go get -u golang.org/x/tools/cmd/cover
	GO111MODULE=on go test $(go_dirs) -tags test -v -covermode=count -coverprofile=$(coverage_out)
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
	rm -rf $(vendor_dir)
