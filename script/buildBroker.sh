#!/bin/bash
echo "Build broker!"
cd ../protocol/
protoc --go_out=. *.proto
go build -o ../bin/broker  ../apps/broker/main.go
cd ../
if [ $# == 0 ];then
    ./bin/broker
else
    ./bin/broker -addr $1
fi

