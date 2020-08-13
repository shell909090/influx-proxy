### Makefile ---

## Author: Shell.Xu
## Version: $Id: Makefile,v 0.0 2017/01/17 03:44:24 shell Exp $
## Copyright: 2017, Eleme <zhixiang.xu@ele.me>, BizSeer <chengshiwen0103@gmail.com>
## License: MIT
## Keywords:
## X-URL:

export GO_BUILD=GO111MODULE=on go build -o bin/influx-proxy -ldflags "-s -X main.GitCommit=$(shell git rev-parse --short HEAD) -X 'main.BuildTime=$(shell date '+%Y-%m-%d %H:%M:%S')'"

all: build

build:
	mkdir -p bin
	$(GO_BUILD)

linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO_BUILD)

test:
	go test -v github.com/chengshiwen/influx-proxy/backend

bench:
	go test -bench=. -run=none github.com/chengshiwen/influx-proxy/backend

run:
	go run main.go

lint:
	golangci-lint run --enable=golint --disable=errcheck --disable=typecheck
	goimports -l -w .
	go fmt ./...
	go vet ./...

clean:
	rm -rf bin data log

### Makefile ends here
