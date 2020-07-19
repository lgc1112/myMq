package main
import (
	"flag"
	"fmt"
)
import "../../consumer"
func main() {
	addr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")
	consumer, err := consumer.NewConsumer(*addr)
	if err != nil {
		fmt.Println("err")
	}
	err = consumer.Connect2Broker()
	if err != nil {
		fmt.Println("err")
	}
	consumer.GetTopicPartion("fff")
	consumer.Subscribe("fff")
	consumer.ReadLoop()
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}

