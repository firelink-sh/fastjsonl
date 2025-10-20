.PHONY: build
build:
	@uv run maturin develop

.PHONY: build-release
build-release:
	@uv run maturin develop --release

.PHONY: clean
clean:
	@rm -rf target/
	@rm -rf dist/
	@rm -f python/fastjsonl/*.so

.PHONY: clean-build
clean-build: clean build

.PHONY: clean-build-release
clean-build-release: clean build-release
