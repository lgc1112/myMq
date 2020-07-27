package main
import (
	"../../mylib/myLogger"
	"../../protocol"
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
	consumer1, err := consumer.NewConsumer(brokerAddrs,"group0")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = consumer1.SubscribeTopic("fff")
	if err != nil {
		myLogger.Logger.Print(err)
		os.Exit(1)
	}
	//consumer.CommitReadyNum(10)

	var myHandle myHandle
	consumer1.ReadLoop(myHandle)
	fmt.Println("bye")
}

type myHandle struct {}
func (h myHandle) ProcessMsg(message *protocol.Server2Client){
	myLogger.Logger.Print("Consumer receive data is :", string(message.Msg.Msg))
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