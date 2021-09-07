#!/bin/sh

if [[ "${1#-}" != "$1" ]]; then
    set -- influx-proxy "$@"
fi

exec "$@"
