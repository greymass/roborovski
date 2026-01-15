# Roborovski Workspace Makefile

# Configuration
BINDIR := bin
SERVICES := actionindex coreverify apiproxy streamproxy coreindex txindex
NPROC := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# All modules (libraries + services) for tagging
MODULES := $(shell find . -name "go.mod" -type f | sed 's|./||; s|/go.mod||' | sort)

# =============================================================================
# Main Targets
# =============================================================================

.PHONY: help
help: ## Show this help message
	@echo "Roborovski Workspace Commands:"
	@echo ""
	@echo "Main:"
	@grep -E '^(build|install|clean|test|verify|tidy):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'
	@echo ""
	@echo "Individual Services (build/<name>, install/<name>):"
	@echo "  $(SERVICES)" | fold -s -w 70 | sed 's/^/  /'
	@echo ""
	@echo "Release:"
	@grep -E '^(tag-list|tag|tag-push|release):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'
	@echo ""
	@echo "Other:"
	@grep -E '^(uninstall|list):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all services to ./bin
	@$(MAKE) -j$(NPROC) $(addprefix build/,$(SERVICES)) --no-print-directory
	@echo ""
	@echo "✅ All services built to ./$(BINDIR)/"

.PHONY: install
install: ## Install all services to GOPATH/bin
	@$(MAKE) -j$(NPROC) $(addprefix install/,$(SERVICES)) --no-print-directory
	@echo ""
	@echo "✅ All services installed"

.PHONY: clean
clean: ## Remove build artifacts
	@echo "Cleaning build artifacts..."
	@rm -f $(BINDIR)/*[!.gitkeep]
	@for mod in $$(awk '/use \(/{flag=1;next}/\)/{flag=0}flag' go.work | tr -d '\t'); do \
		(cd $$mod && go clean) || true; \
	done
	@echo "✅ Clean complete"

.PHONY: test
test: ## Run tests for all modules
	@echo "Testing all workspace modules..."
	@for mod in $$(awk '/use \(/{flag=1;next}/\)/{flag=0}flag' go.work | tr -d '\t'); do \
		echo ""; \
		echo "==> Testing $$mod"; \
		(cd $$mod && go test ./...) || exit 1; \
	done
	@echo ""
	@echo "✅ All tests passed"

.PHONY: verify
verify: ## Full validation (tidy + build + test)
	@$(MAKE) tidy --no-print-directory
	@$(MAKE) build --no-print-directory
	@$(MAKE) test --no-print-directory
	@echo ""
	@echo "✅ Workspace verification complete"

.PHONY: tidy
tidy: ## Sync workspace dependencies
	@echo "Syncing workspace..."
	@go work sync
	@echo "✅ Workspace synced"

.PHONY: coverage
coverage: ## Show test coverage for all modules
	@echo "Module|Coverage"
	@echo "------|--------"
	@for lib in libraries/*/; do \
		name=$$(basename "$$lib"); \
		cov=$$(go test -cover "./$$lib..." 2>/dev/null | grep -o '[0-9]*\.[0-9]*%' | head -1); \
		[ -z "$$cov" ] && cov="0.0%"; \
		echo "$$name|$$cov"; \
	done
	@for svc in services/*/; do \
		name=$$(basename "$$svc"); \
		cov=$$(go test -cover "./$$svc..." 2>/dev/null | grep -o '[0-9]*\.[0-9]*%' | tail -1); \
		[ -z "$$cov" ] && cov="0.0%"; \
		echo "$$name|$$cov"; \
	done

# =============================================================================
# Other Targets
# =============================================================================

.PHONY: uninstall
uninstall: ## Remove installed binaries from GOPATH/bin
	@echo "Uninstalling services..."
	@GOPATH=$${GOPATH:-$$(go env GOPATH)}; \
	for svc in $(SERVICES); do \
		if [ -f "$$GOPATH/bin/$$svc" ]; then \
			echo "==> Removing $$svc"; \
			rm -f "$$GOPATH/bin/$$svc"; \
		fi; \
	done
	@echo "✅ Uninstall complete"

.PHONY: list
list: ## List all modules in workspace
	@echo "Workspace modules:"
	@awk '/use \(/{flag=1;next}/\)/{flag=0}flag' go.work | tr -d '\t'

# =============================================================================
# Helper Functions
# =============================================================================

# Check if rebuild is needed: $(call needs_rebuild,binary,service_dir)
define needs_rebuild
$(shell \
	target=$(BINDIR)/$(1); \
	if [ ! -f "$$target" ]; then \
		echo "1"; \
	elif [ "$$(find $(2) -name '*.go' -newer $$target 2>/dev/null | wc -l | tr -d ' ')" != "0" ]; then \
		echo "1"; \
	elif [ "$$(find libraries -name '*.go' -newer $$target 2>/dev/null | wc -l | tr -d ' ')" != "0" ]; then \
		echo "1"; \
	else \
		echo "0"; \
	fi \
)
endef

# =============================================================================
# Individual Build Targets (build/<service>)
# =============================================================================

.PHONY: build/actionindex
build/actionindex:
	@if [ "$(call needs_rebuild,actionindex,services/actionindex)" = "1" ]; then \
		echo "==> Building actionindex (CGO_ENABLED=0)"; \
		CGO_ENABLED=0 go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/actionindex ./services/actionindex/cmd/actionindex; \
	else \
		echo "==> actionindex is up to date"; \
	fi

.PHONY: build/coreverify
build/coreverify:
	@if [ "$(call needs_rebuild,coreverify,services/coreverify)" = "1" ]; then \
		echo "==> Building coreverify"; \
		go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/coreverify ./services/coreverify/cmd/coreverify; \
	else \
		echo "==> coreverify is up to date"; \
	fi

.PHONY: build/apiproxy
build/apiproxy:
	@if [ "$(call needs_rebuild,apiproxy,services/apiproxy)" = "1" ]; then \
		echo "==> Building apiproxy"; \
		go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/apiproxy ./services/apiproxy/cmd/apiproxy; \
	else \
		echo "==> apiproxy is up to date"; \
	fi

.PHONY: build/streamproxy
build/streamproxy:
	@if [ "$(call needs_rebuild,streamproxy,services/streamproxy)" = "1" ]; then \
		echo "==> Building streamproxy"; \
		go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/streamproxy ./services/streamproxy/cmd/streamproxy; \
	else \
		echo "==> streamproxy is up to date"; \
	fi

.PHONY: build/coreindex
build/coreindex:
	@if [ "$(call needs_rebuild,coreindex,services/coreindex)" = "1" ]; then \
		echo "==> Building coreindex"; \
		go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/coreindex ./services/coreindex/cmd/coreindex; \
	else \
		echo "==> coreindex is up to date"; \
	fi

.PHONY: build/txindex
build/txindex:
	@if [ "$(call needs_rebuild,txindex,services/txindex)" = "1" ]; then \
		echo "==> Building txindex"; \
		go build -ldflags "-X main.Version=$(VERSION)" -o $(BINDIR)/txindex ./services/txindex/cmd/txindex; \
	else \
		echo "==> txindex is up to date"; \
	fi

# =============================================================================
# Individual Install Targets (install/<service>)
# =============================================================================

.PHONY: install/actionindex
install/actionindex:
	@echo "==> Installing actionindex (CGO_ENABLED=0)"
	@CGO_ENABLED=0 go install ./services/actionindex/cmd/actionindex

.PHONY: install/coreverify
install/coreverify:
	@echo "==> Installing coreverify"
	@go install ./services/coreverify/cmd/coreverify

.PHONY: install/apiproxy
install/apiproxy:
	@echo "==> Installing apiproxy"
	@go install ./services/apiproxy/cmd/apiproxy

.PHONY: install/streamproxy
install/streamproxy:
	@echo "==> Installing streamproxy"
	@go install ./services/streamproxy/cmd/streamproxy

.PHONY: install/coreindex
install/coreindex:
	@echo "==> Installing coreindex"
	@go install ./services/coreindex/cmd/coreindex

.PHONY: install/txindex
install/txindex:
	@echo "==> Installing txindex"
	@go install ./services/txindex/cmd/txindex

# =============================================================================
# Release Targets
# =============================================================================

.PHONY: tag-list
tag-list: ## List all tags that would be created for VERSION
	@if [ "$(VERSION)" = "dev" ] || [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make tag-list VERSION=v1.0.0-beta1"; \
		exit 1; \
	fi
	@echo "Tags to be created for $(VERSION):"
	@echo ""
	@echo "  $(VERSION)  (root tag)"
	@for mod in $(MODULES); do \
		echo "  $$mod/$(VERSION)"; \
	done
	@echo ""
	@echo "Total: $$(echo $(MODULES) | wc -w | tr -d ' ') module tags + 1 root tag"

.PHONY: tag
tag: ## Create git tags for all modules (requires VERSION=vX.Y.Z)
	@if [ "$(VERSION)" = "dev" ] || [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make tag VERSION=v1.0.0-beta1"; \
		exit 1; \
	fi
	@if ! echo "$(VERSION)" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+'; then \
		echo "Error: VERSION must be in format vX.Y.Z (e.g., v1.0.0-beta1)"; \
		exit 1; \
	fi
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Error: Working directory is not clean. Commit or stash changes first."; \
		exit 1; \
	fi
	@echo "Creating tags for $(VERSION)..."
	@git tag -a "$(VERSION)" -m "Release $(VERSION)" 2>/dev/null || echo "  $(VERSION) already exists, skipping"
	@for mod in $(MODULES); do \
		tag="$$mod/$(VERSION)"; \
		if git rev-parse "$$tag" >/dev/null 2>&1; then \
			echo "  $$tag already exists, skipping"; \
		else \
			git tag -a "$$tag" -m "Release $$mod $(VERSION)"; \
			echo "  Created $$tag"; \
		fi; \
	done
	@echo ""
	@echo "✅ Tags created. Run 'make tag-push' to push to origin."

.PHONY: tag-push
tag-push: ## Push all tags to origin
	@echo "Pushing all tags to origin..."
	@git push origin --tags
	@echo ""
	@echo "✅ Tags pushed to origin"

.PHONY: tag-delete
tag-delete: ## Delete all tags for VERSION (local only, requires VERSION=vX.Y.Z)
	@if [ "$(VERSION)" = "dev" ] || [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make tag-delete VERSION=v1.0.0-beta1"; \
		exit 1; \
	fi
	@echo "Deleting local tags for $(VERSION)..."
	@git tag -d "$(VERSION)" 2>/dev/null || true
	@for mod in $(MODULES); do \
		git tag -d "$$mod/$(VERSION)" 2>/dev/null || true; \
	done
	@echo ""
	@echo "✅ Local tags deleted. Remote tags must be deleted manually if needed."

.PHONY: release
release: verify ## Full release: verify + tag + push (requires VERSION=vX.Y.Z)
	@if [ "$(VERSION)" = "dev" ] || [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=v1.0.0-beta1"; \
		exit 1; \
	fi
	@$(MAKE) tag VERSION=$(VERSION) --no-print-directory
	@$(MAKE) tag-push --no-print-directory
	@echo ""
	@echo "✅ Release $(VERSION) complete"
