package main

import (
	"../../mylib/myLogger"
	"../../protocol"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
import "../../consumer"


var sum int32
const  testTime = 20000 * time.Second//测试时间
const consumerNum = 1
func main() {
	addr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	flag.Parse() //解析参数
	fmt.Println("Hello, World!")

	if *addr == "0.0.0.0:12345" {
		*addr = getIntranetIp() + ":12345"
	}
	flag.Parse() //解析参数

	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	for i := 0; i < consumerNum; i++{
		consumer, err := consumer.NewConsumer(brokerAddrs,"group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
		myHandle := myHandle{i, 0}
		wg.Add(1)
		go consumer.ReadLoop(myHandle)
		//time.Sleep(100 * time.Millisecond)
		myLogger.Logger.PrintDebug("NewConsumer:", i)
	}
	myLogger.Logger.PrintDebug("NewConsumer finished:")
	wg.Wait()
	//timeTicker := time.NewTicker(testTime)//测试开始
	//atomic.StoreInt32(&sum, 0)
	//<- timeTicker.C
	//seconds := int32(testTime / time.Second)
	//myLogger.Logger.PrintfDebug("consumerNum: %d, test time : %d , send times : %d, qps : %d", consumerNum, seconds, sum, sum / seconds)

	//
	//consumer1, err := consumer.NewConsumer(brokerAddrs,"group0")
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}
	//err = consumer1.SubscribeTopic("fff")
	//if err != nil {
	//	myLogger.Logger.Print(err)
	//	os.Exit(1)
	//}
	////consumer.CommitReadyNum(10)
	//
	//var myHandle myHandle
	//consumer1.ReadLoop(myHandle)
	//fmt.Println(i)
	//fmt.Println("bye")
}
type myHandle struct {
	id int
	receivedNum int64
}
func (h myHandle) ProcessMsg(msg *protocol.Message) {
	atomic.AddInt32(&sum, 1)
	myLogger.Logger.Printf("Consumer %d receive data is %s:", h.id, string(msg.Msg))
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