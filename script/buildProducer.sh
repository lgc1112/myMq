#!/bin/bash
echo "Build Producer !"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/producer  ../apps/producer/main.go
cd ../
./bin/producer

