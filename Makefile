### Makefile ---

## Author: Shell.Xu
## Version: $Id: Makefile,v 0.0 2017/01/17 03:44:24 shell Exp $
## Copyright: 2017, Eleme <zhixiang.xu@ele.me>
## License: MIT
## Keywords:
## X-URL:

all: build

build:
	mkdir -p bin
	go build -o bin/influx-proxy github.com/chengshiwen/influx-proxy

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/influx-proxy -ldflags "-s" github.com/chengshiwen/influx-proxy

run:
	go run main.go

clean:
	rm -rf bin log


### Makefile ends here
