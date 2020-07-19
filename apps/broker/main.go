package main

import (
	"../../broker"
	"fmt"
)

func main() {
	fmt.Println("Hello, World!")
	//request := &protocol.Request{
	//	Key: protocol.RequestKey_CreatTopic,
	//	Topic: "hhh",
	//	Partition: "74898749",
	//	PartitionNum: 10,
	//}
	//data, err := proto.Marshal(request)
	//if err != nil {
	//	log.Fatal("marshaling error: ", err)
	//}
	//newTest := &protocol.Request{}
	//
	//fmt.Println("2")
	//err = proto.Unmarshal(data, newTest)
	//if err == nil {
	//	fmt.Println(newTest.Key)
	//	fmt.Println(newTest.Partition)
	//	fmt.Println(newTest.Topic)
	//	fmt.Println(newTest.Key)
	//}else{
	//	fmt.Println(err)
	//}
	//fmt.Println("1")

	b, err:= broker.New();
	if err != nil {
		fmt.Println(err);
	}
	b.Main();
	fmt.Println("1")
}
