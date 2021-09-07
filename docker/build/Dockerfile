FROM golang:1.14-alpine as builder

WORKDIR /go/influx-proxy

COPY . .

RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.tuna.tsinghua.edu.cn/g' /etc/apk/repositories && \
    go env -w GOPROXY=https://goproxy.cn,direct

RUN apk add --no-cache git make && make


FROM alpine

COPY --from=builder /go/influx-proxy/bin/influx-proxy /usr/local/bin/

RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.tuna.tsinghua.edu.cn/g' /etc/apk/repositories

RUN apk add --no-cache ca-certificates tzdata bash

EXPOSE 7076

WORKDIR /etc/influx-proxy

COPY proxy.json /etc/influx-proxy

COPY conf/ /etc/influx-proxy

COPY docker/build/entrypoint.sh /usr/local/bin/

ENTRYPOINT ["entrypoint.sh"]

CMD ["influx-proxy"]
