package main
import (
	"flag"
	"fmt"
	"time"
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
	pr.CreatTopic("fff", 1)
	for{
		time.Sleep(1*time.Second)
		pr.Pubilsh("fff", []byte("hahahaha"), 0)
	}
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}

