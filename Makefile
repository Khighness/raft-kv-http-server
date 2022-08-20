GO          = go
PRODUCT     = raft-kv-http-server
GOARCH     := amd64
GO111MODULE = on

gitlab-setup:
	@[ "${CI_JOB_TOKEN}" ] && go env -w GOPRIVATE=git.garena.com && git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@git.garena.com".insteadOf "https://git.garena.com" || ( echo "CI_JOB_TOKEN not found. Skip")

all: gitlab-setup
all: $(shell $(GO) env GOOS)

build:
	env GO111MODULE=${GO111MODULE} GOOS=${GOOS} GOARCH=$(GOARCH) $(GO) build $(EXTFLAGS) -o $(PRODUCT)$(EXT) .

linux: export GOOS=linux
linux: build

darwin: export GOOS=darwin
darwin: build

.PHONY: clean
clean:
	@rm -f $(PRODUCT) $(PRODUCT).elf $(PRODUCT).mach
