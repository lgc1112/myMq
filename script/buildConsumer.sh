#!/bin/bash
echo "Build Consumer !"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/consumer  ../apps/consumer/main.go
cd ../
if [ $# == 0 ];then
    ./bin/consumer
else
    ./bin/consumer -addr $1
fi
