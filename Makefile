### Makefile --- 

## Author: shell@xps13
## Version: $Id: Makefile,v 0.0 2017/01/17 03:44:24 shell Exp $
## Keywords: 
## X-URL: 

all: build

build:
	mkdir -p bin
	go build -o bin/influx-proxy github.com/shell909090/influx-proxy/main

test:
	go test -v github.com/shell909090/influx-proxy/backend
	# go test github.com/shell909090/influx-proxy/service
	# go test github.com/shell909090/influx-proxy/main

clean:
	rm -rf bin


### Makefile ends here
