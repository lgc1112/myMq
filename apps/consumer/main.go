package main
import (
	"../../mylib/myLogger"
	"flag"
	"fmt"
)
import "../../consumer"
func main() {
	addr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")
	consumer, err := consumer.NewConsumer(*addr, "group0")
	if err != nil {
		fmt.Println(err)
	}
	err = consumer.Connect2Broker()
	if err != nil {
		myLogger.Logger.Print(err)
	}
	err = consumer.SubscribeTopic("fff")
	if err != nil {
		myLogger.Logger.Print(err)
	}
	consumer.ReadLoop()
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}
