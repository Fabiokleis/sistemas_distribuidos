GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_DIR=out

PROJECTS := $(shell ls cmd)
BINARIES := $(addprefix $(BINARY_DIR)/,$(PROJECTS))
RUN_PROJECTS := $(addprefix run-,$(PROJECTS))

.PHONY: all build clean run $(PROJECTS)
all: build

build: $(PROJECTS)

$(PROJECTS): %: $(BINARY_DIR)/%

run: $(RUN_PROJECTS)

$(BINARY_DIR)/%: cmd/%/*.go
	@echo "building $@ ..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $@ ./cmd/$*

run-%: $(BINARY_DIR)/%
	@echo "executing $* ..."
	@./$(BINARY_DIR)/$*

test:
	$(GOCMD) test -v -count=1 ./cmd/client/... ./internal/...

test-%:
	$(GOCMD) test -v -count=1 ./cmd/$*/...

clean:
	@echo "removing $(BINARY_DIR) ..."
	@rm -rf $(BINARY_DIR)
