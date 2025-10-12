.PHONY: build
build:
	@uv run maturin develop

.PHONY: clean
clean:
	@rm -rf target/
	@rm -rf dist/

.PHONY: clean-build
clean-build: clean build
