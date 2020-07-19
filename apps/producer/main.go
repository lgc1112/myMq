package main
import (
	"flag"
	"fmt"
)
import "../../producer"
func main() {
	addr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")
	pr, err := producer.NewProducer(*addr)
	if err != nil {
		fmt.Println("err")
	}
	err = pr.Connect2Broker()
	if err != nil {
		fmt.Println("err")
	}
	pr.CreatTopic("fff", 10)
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}

