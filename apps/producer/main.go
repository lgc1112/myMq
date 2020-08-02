package main

import (
	"../../mylib/myLogger"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
import "../../producer"

var sum int32
const  testTime = 10000 * time.Second//测试时间
const producerNum = 1
const partitionNum = 1
func main() {
	fmt.Println("producer start")
	brokerAddr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	flag.Parse() //解析参数

	if *brokerAddr == "0.0.0.0:12345" {
		*brokerAddr = getIntranetIp() + ":12345" //真实ip
	}
	flag.Parse() //解析参数

	brokerAddrs := []string{*brokerAddr}
	//signal.Notify(b.exitSignal, os.Interrupt, os.Kill)
	//signal.Notify(b.exitSignal)


	var wg sync.WaitGroup
	for i := 0; i < producerNum; i++{
		p, err := producer.NewProducer(brokerAddrs)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0{
			p.CreatTopic("fff", partitionNum)
		}
		wg.Add(1)
		go func() {
			producerHandle(p)
			//atomic.AddInt64(&sum, times)
			wg.Done()
		}()

	}

	//wg.Wait()
	starTime := time.Now()
	timeTicker := time.NewTicker(testTime)
	atomic.StoreInt32(&sum, 0)
	myLogger.Logger.PrintfDebug("%d", sum )

	<- timeTicker.C
	endTime := time.Now()
	seconds := int64(testTime / time.Second)
	seconds = int64(endTime.Sub(starTime).Seconds())
	myLogger.Logger.PrintfDebug("partitionNum %d, producerNum: %d, test time : %d , send times : %d, qps : %d",partitionNum, producerNum, seconds, sum, int64(sum) / seconds)
	//myLogger.Logger.PrintfDebug("%d", seconds )
	//exitCh := make(chan error)
	//<-exitCh
	//fmt.Println("bye")
}

func producerHandle(p *producer.Producer)  int64{
	var i int64
	//timeTicker := time.NewTicker(testTime)
	//timeTickerChan := timeTicker.C
	for{
		select {
		//case <- timeTickerChan:
		//	goto exit
		default:
			s := fmt.Sprintf("hello : %d", i)
			i++
			time.Sleep(1*time.Second)
			err := p.Pubilsh("fff", []byte(s), 0)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			atomic.AddInt32(&sum, 1)
		}
	}
//exit:
//	myLogger.Logger.Print("producer stop: ", i)
	//timeTicker.Stop()
	return i
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