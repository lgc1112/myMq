package main

import (
	"../../mylib/myLogger"
	"../../protocol"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "../../consumer"

var sum int32
var latency int64

const testTime = 20000 * time.Second //测试时间
var consumerNum = 1                  //消费者数
var partitionNum = 6                 //生产者数
var reCreateTopic = true             //是否需要重建topic true  false
func main() {
	fmt.Println("consumer start")

	//解析参数
	brokerAddr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	cNum := flag.Int("consumerNum", consumerNum, "int")
	pNum := flag.Int("partitionNum", partitionNum, "int")
	rC := flag.Bool("reCreateTopic", reCreateTopic, "bool")
	flag.Parse() //解析参数

	consumerNum = *cNum
	partitionNum = *pNum
	reCreateTopic = *rC

	fmt.Println("consumerNum:", consumerNum, " partitionNum:", partitionNum, " reCreateTopic", reCreateTopic)

	host, port, _ := net.SplitHostPort(*brokerAddr)
	if host == "0.0.0.0" { //转换为本地ip
		*brokerAddr = getIntranetIp() + ":" + port //真实ip
	}

	//StressTest(brokerAddr)
	//latencyTest(brokerAddr)

	NormalTest(brokerAddr)
}

func NormalTest(addr *string) {
	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	exitChan := make(chan bool)

	for i := 0; i < consumerNum; i++ {
		consumer, err := consumer.NewConsumer(brokerAddrs, "group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		//myHandle := myHandle2{i}
		wg.Add(1)
		go func() {
			consumer.ReadLoop(nil, exitChan) //处理接收消息
			wg.Done()
		}()
		myLogger.Logger.Print("NewConsumer:", i)
		if i == 0 && reCreateTopic {
			consumer.DeleteTopic("fff") //先删除原来的分区
			consumer.CreatTopic("fff", int32(partitionNum))
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
	}

	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号

	s := <-exitSignal //退出信号来了
	myLogger.Logger.Print("exitSignal:", s)
	close(exitChan) //关闭退出管道，通知所有协程退出
	wg.Wait()       //等待退出
	myLogger.Logger.Print("NewConsumer finished:")
}

//压力测试
func StressTest(addr *string) {
	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	exitChan := make(chan bool)

	for i := 0; i < consumerNum; i++ {
		consumer, err := consumer.NewConsumer(brokerAddrs, "group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		myHandle := myHandle{i, 0}
		wg.Add(1)
		go func() {
			consumer.ReadLoop(myHandle, exitChan) //处理接收消息
			wg.Done()
		}()
		myLogger.Logger.Print("NewConsumer:", i)
		if i == 0 && reCreateTopic {
			consumer.DeleteTopic("fff") //先删除原来的分区
			consumer.CreatTopic("fff", int32(partitionNum))
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
	}

	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号

	timeTicker := time.NewTicker(time.Second) //每秒触发一次
	myLogger.Logger.Printf("%d", sum)
	lastSecendSum := sum
	for {
		select {
		case s := <-exitSignal: //退出信号来了
			myLogger.Logger.Print("exitSignal:", s)
			close(exitChan) //关闭退出管道，通知所有协程退出
			goto exit
		case <-timeTicker.C:
			curSum := atomic.LoadInt32(&sum)
			myLogger.Logger.PrintfDebug("接收速率: %d / s, 当前接收总量 %d", curSum-lastSecendSum, curSum)
			lastSecendSum = curSum
		}
	}
exit:
	wg.Wait() //等待退出

	myLogger.Logger.PrintfDebug("消费者数量: %d    接收总数: %d", consumerNum, sum)
	myLogger.Logger.Print("NewConsumer finished:")
	wg.Wait()
}

type myHandle struct {
	id          int
	receivedNum int64
}

func (h myHandle) ProcessMsg(msg *protocol.Message) {
	atomic.AddInt32(&sum, 1)
	myLogger.Logger.Printf("Consumer %d receive data is %s:", h.id, string(msg.Msg))
}

//压力测试
func latencyTest(addr *string) {
	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	exitChan := make(chan bool)

	for i := 0; i < consumerNum; i++ {
		consumer, err := consumer.NewConsumer(brokerAddrs, "group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		myHandle := LatencyTestHandle{i}
		wg.Add(1)
		go func() {
			consumer.ReadLoop(myHandle, exitChan) //处理接收消息
			wg.Done()
		}()
		myLogger.Logger.Print("NewConsumer:", i)
		if i == 0 && reCreateTopic {
			consumer.DeleteTopic("fff") //先删除原来的分区
			consumer.CreatTopic("fff", int32(partitionNum))
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
	}

	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号

	s := <-exitSignal //退出信号来了
	myLogger.Logger.Print("exitSignal:", s)
	close(exitChan) //关闭退出管道，通知所有协程退出
	wg.Wait()       //等待退出
	if sum == 0 {
		myLogger.Logger.PrintfDebug2("消息数量: 0")
	} else {
		myLogger.Logger.PrintfDebug2("消息数量: %d   平均延迟: %.2f ms", consumerNum, float32(latency/int64(sum))/1000/1000)
	}
}

type LatencyTestHandle struct {
	id int
}

func (h LatencyTestHandle) ProcessMsg(msg *protocol.Message) {
	sendTime, err := strconv.Atoi(string(msg.Msg))
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	receiveTime := int(time.Now().UnixNano())

	atomic.AddInt32(&sum, 1) //记录总数，记录平均延迟
	atomic.AddInt64(&latency, int64(receiveTime-sendTime))
	//time.Now().UnixNano() / 1e6
	myLogger.Logger.PrintfDebug2("Consumer %d sendTime %d, receiveTime %d latency %.2f ms", h.id, sendTime, receiveTime, float32(receiveTime-sendTime)/1000/1000)
}

func getIntranetIp() string {
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

//func NormalTest(addr *string) {
//	brokerAddrs := []string{*addr}
//	var wg sync.WaitGroup
//	for i := 0; i < consumerNum; i++ {
//		consumer, err := consumer.NewConsumer(brokerAddrs, "group0")
//		if err != nil {
//			fmt.Println(err)
//			os.Exit(1)
//		}
//		if i == 0 && reCreateTopic {
//			consumer.DeleteTopic("fff") //先删除原来的分区
//			//time.Sleep(1000 * time.Millisecond)
//			consumer.CreatTopic("fff", int32(partitionNum))
//			//time.Sleep(3000 * time.Millisecond)
//		}
//		err = consumer.SubscribeTopic("fff")
//		if err != nil {
//			myLogger.Logger.Print(err)
//			os.Exit(1)
//		}
//		myHandle := myHandle{i, 0}
//		wg.Add(1)
//		go consumer.ReadLoop(myHandle, nil) //处理接收消息
//		//time.Sleep(100 * time.Millisecond)
//		myLogger.Logger.PrintDebug("NewConsumer:", i)
//	}
//	myLogger.Logger.PrintDebug("NewConsumer finished:")
//	wg.Wait()
//}
