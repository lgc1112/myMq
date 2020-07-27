package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)
import "../../producer"
func main() {
	brokerAddr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	flag.Parse() //解析参数

	if *brokerAddr == "0.0.0.0:12345" {
		*brokerAddr = getIntranetIp() + ":12345"
	}
	flag.Parse() //解析参数

	brokerAddrs := []string{*brokerAddr}
	p, err := producer.NewProducer(brokerAddrs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	p.CreatTopic("fff", 1)
	var i int32
	for ;;i++{
		s := fmt.Sprintf("hello : %d", i)

		time.Sleep(1*time.Second)
		p.Pubilsh("fff", []byte(s), 0)
	}
	//exitCh := make(chan error)
	//<-exitCh
	//fmt.Println("bye")
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