package main
import (
	"flag"
	"fmt"
	"time"
)
import "../../producer"
func main() {
	brokerAddr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")
	pr, err := producer.NewProducer(*brokerAddr)
	if err != nil {
		fmt.Println("err")
	}
	for{

		err = pr.Connect2Broker()
		if err != nil {
			fmt.Println("err")
		}else{
			break
		}
	}
	pr.CreatTopic("fff", 10)
	for{
		time.Sleep(1*time.Second)
		pr.Pubilsh("fff", []byte("hahahaha"), 0)
	}
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}

