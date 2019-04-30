#!/bin/bash

set -ex

dir="$(mktemp -d)"

GOOS=linux go build -o "$dir/andbserver" ./cmd/andbserver
cat >"$dir/Dockerfile" <<EOF
FROM alpine
WORKDIR /etc/andb
COPY andbserver .
EXPOSE 8080
CMD ["./andbserver", "-logfile", "andb.log"]
EOF

docker build -t ankeesler/andb "$dir"

rm -rf "$dir"
