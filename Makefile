GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_DIR=out

PROJECTS := $(shell ls cmd)
BINARIES := $(addprefix $(BINARY_DIR)/,$(PROJECTS))
RUN_PROJECTS := $(addprefix run-,$(PROJECTS))

.PHONY: all build proto clean run $(PROJECTS)
all: build

build: proto $(PROJECTS)

$(PROJECTS): %: $(BINARY_DIR)/%

run: $(RUN_PROJECTS)

$(BINARY_DIR)/%: proto cmd/%/*.go
	@echo "building $@ ..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $@ ./cmd/$*

run-%: $(BINARY_DIR)/%
	@echo "executing $* ..."
	@./$(BINARY_DIR)/$*

proto:
	@echo "compiling protobuf ..."
	protoc --go_out=. --go_opt=module=promocao ./proto/events.proto

test: build
	$(GOCMD) test -p 1 -v -count=1 ./... ./internal/...

test-%: $(BINARY_DIR)/%
	$(GOCMD) test -v -count=1 ./internal/$*/...

clean:
	@echo "removing $(BINARY_DIR) ..."
	@rm -rf $(BINARY_DIR)
	@echo "removing generated protobuf files ..."
	@rm -f internal/models/proto/events/*.pb.go
