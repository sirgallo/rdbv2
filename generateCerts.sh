#!/bin/bash

echo "creating certs directories under $HOME"

ROOT=$HOME/rdb/certs
HAPROXY=$ROOT/haproxy

mkdir -p $HAPROXY

echo "generating root ca cert"
openssl genrsa -out $ROOT/ca.key 2048
openssl req -x509 -new -nodes -key $ROOT/ca.key -sha256 -days 1024 -out $ROOT/ca.crt

echo "generating haproxy certs"
openssl genrsa -out $HAPROXY/$HOSTNAME.key 2048
openssl req -new -key $HAPROXY/$HOSTNAME.key -out $HAPROXY/$HOSTNAME.csr
openssl x509 -req -in $HAPROXY/$HOSTNAME.csr -CA $ROOT/ca.crt -CAkey $ROOT/ca.key -CAcreateserial -out $HAPROXY/$HOSTNAME.crt -days 365 -sha256
cat $HAPROXY/$HOSTNAME.key $HAPROXY/$HOSTNAME.crt > $HAPROXY/$HOSTNAME.pem