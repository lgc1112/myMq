#!/bin/bash
echo "Hello World !"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/broker  ../apps/broker/main.go
cd ../
./bin/broker
