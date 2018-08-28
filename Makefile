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
	go build -o bin/influx-proxy github.com/chengshiwen/influx-proxy/service

test:
	go test -v github.com/chengshiwen/influx-proxy/backend

bench:
	go test -bench=. github.com/chengshiwen/influx-proxy/backend

clean:
	rm -rf bin


### Makefile ends here
