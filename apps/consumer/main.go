package main

import (
	"../../mylib/myLogger"
	"../../protocol"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)
import "../../consumer"


var sum int32
const  testTime = 20000 * time.Second//测试时间
var consumerNum = 10
var partitionNum = 10
var reCreateTopic = true //是否需要重建topic
func main() {
	fmt.Println("consumer start")

	//解析参数
	brokerAddr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	cNum := flag.Int("consumerNum", consumerNum, "int")
	pNum := flag.Int("partitionNum", partitionNum, "int")
	rC := flag.Bool("reCreateTopic", false, "bool")
	flag.Parse() //解析参数

	consumerNum = *cNum
	partitionNum = *pNum
	reCreateTopic = *rC

	fmt.Println("consumerNum:", consumerNum, " partitionNum:", partitionNum, " reCreateTopic", reCreateTopic)

	host, port, _ := net.SplitHostPort(*brokerAddr)
	if host == "0.0.0.0" { //转换为本地ip
		*brokerAddr = getIntranetIp() + ":" + port//真实ip
	}

	StressTest(brokerAddr)
}

type myHandle struct {
	id int
	receivedNum int64
}

func  NormalTest(addr *string)  {
	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	for i := 0; i < consumerNum; i++{
		consumer, err := consumer.NewConsumer(brokerAddrs,"group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0 && reCreateTopic{
			consumer.DeleteTopic("fff") //先删除原来的分区
			time.Sleep(1000 * time.Millisecond)
			consumer.CreatTopic("fff", int32(partitionNum))
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
		myHandle := myHandle{i, 0}
		wg.Add(1)
		go consumer.ReadLoop(myHandle, nil) //处理接收消息
		//time.Sleep(100 * time.Millisecond)
		myLogger.Logger.PrintDebug("NewConsumer:", i)
	}
	myLogger.Logger.PrintDebug("NewConsumer finished:")
	wg.Wait()
}

//压力测试
func  StressTest(addr *string) {
	brokerAddrs := []string{*addr}
	var wg sync.WaitGroup
	exitChan := make(chan bool)

	for i := 0; i < consumerNum; i++ {
		consumer, err := consumer.NewConsumer(brokerAddrs, "group0")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0 && reCreateTopic {
			consumer.DeleteTopic("fff") //先删除原来的分区
			consumer.CreatTopic("fff", int32(partitionNum))
		}
		err = consumer.SubscribeTopic("fff")
		if err != nil {
			myLogger.Logger.Print(err)
			os.Exit(1)
		}
		myHandle := myHandle{i, 0}
		wg.Add(1)
		go func() {
			consumer.ReadLoop(myHandle, exitChan) //处理接收消息
			wg.Done()
		}()
		myLogger.Logger.Print("NewConsumer:", i)
	}

	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill)//监听信号


	//starTime := time.Now()
	timeTicker := time.NewTicker(time.Second) //每秒触发一次
	//atomic.StoreInt32(&sum, 0)
	myLogger.Logger.Printf("%d", sum )
	lastSecendSum := sum
	for{
		select {
		case s := <- exitSignal: //退出信号来了
			myLogger.Logger.Print("exitSignal:", s)
			close(exitChan) //关闭退出管道，通知所有协程退出
			goto exit
		case <- timeTicker.C:
			curSum := atomic.LoadInt32(&sum)
			myLogger.Logger.PrintfDebug("接收速率: %d / s, 当前接收总量 %d", curSum - lastSecendSum, curSum)
			lastSecendSum = curSum
		}
	}
exit:
	wg.Wait()//等待退出

	//endTime := time.Now()
	//seconds := int64(endTime.Sub(starTime).Seconds())
	myLogger.Logger.PrintfDebug("消费者数量: %d    接收总数: %d", consumerNum,  sum)
	myLogger.Logger.Print("NewConsumer finished:")
	wg.Wait()
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