GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_DIR=out

PROJECTS := $(shell ls cmd)
RUN_PROJECTS := $(addprefix run-,$(PROJECTS))

.PHONY: all build clean $(PROJECTS)

all: build

build: $(PROJECTS)

run-%: %
	@echo "executing $* ..."
	./$(BINARY_DIR)/$*

run: $(RUN_PROJECTS)

$(PROJECTS):
	@echo "building $@ ..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $(BINARY_DIR)/$@ ./cmd/$@

clean:
	rm -rf $(BINARY_DIR)
