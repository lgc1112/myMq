#!/bin/bash
echo "Build broker!"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/broker  ../apps/broker/main.go
cd ../
./bin/broker
