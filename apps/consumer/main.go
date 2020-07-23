package main
import (
	"../../mylib/myLogger"
	"flag"
	"fmt"
	"net"
	"os"
)
import "../../consumer"
func main() {
	addr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")

	if *addr == "0.0.0.0:12345" {
		*addr = getIntranetIp() + ":12345"
	}
	flag.Parse() //解析参数

	brokerAddrs := []string{*addr}
	consumer, err := consumer.NewConsumer(brokerAddrs,"group0")
	if err != nil {
		fmt.Println(err)
	}

	err = consumer.Connect2Brokers()
	if err != nil {
		myLogger.Logger.Print(err)
		os.Exit(1)
	}
	err = consumer.SubscribeTopic("fff")
	if err != nil {
		myLogger.Logger.Print(err)
		os.Exit(1)
	}
	consumer.ReadLoop()
	exitCh := make(chan error)
	<-exitCh
	fmt.Println("bye")
}

func getIntranetIp() string{
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}

		}
	}
	return "0.0.0.0"
}