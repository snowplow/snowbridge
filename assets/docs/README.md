# Documentation examples

The assets in this directory are referenced directly in the documentation for the project on `docs.snowplow.io`.

The configuration files represent API docs for the project. Minimal examples should contain only required options, and full examples should contain all available options.

The tests in `docs` test these examples. They should fail if any examples don't build, or if 'full' examples are missing an option. Note that due to how the tests were written, full examples will also fail if the entry exists, but the value is the zero value for that option - we should provide non-zero values in the examples to avoid that complication.

To ensure that documentation is kept up to date, with every release these examples should be updated with any new configuration options introduced, and a comment explaining the option.