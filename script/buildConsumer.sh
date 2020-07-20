#!/bin/bash
echo "Build Consumer !"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/consumer  ../apps/consumer/main.go
cd ../
./bin/producer

