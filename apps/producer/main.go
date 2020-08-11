package main

import (
	"../../mylib/myLogger"
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
import "../../producer"

var sum int32

const testTime = 10000 * time.Second //测试时间
var producerNum = 1
var msgLen = 10
var reCreateTopic = false //是否需要重建topic
var partitionNum = 1

func main() {
	fmt.Println("producer start")
	//解析参数
	brokerAddr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	prdNum := flag.Int("producerNum", producerNum, "int")
	parNum := flag.Int("partitionNum", partitionNum, "int")
	rC := flag.Bool("reCreateTopic", reCreateTopic, "bool")
	mL := flag.Int("msgLen", msgLen, "int")
	flag.Parse() //解析参数
	producerNum = *prdNum
	partitionNum = *parNum
	reCreateTopic = *rC
	msgLen = *mL

	host, port, _ := net.SplitHostPort(*brokerAddr)
	if host == "0.0.0.0" { //转换为本地ip
		*brokerAddr = getIntranetIp() + ":" + port //真实ip
	}

	brokerAddrs := []string{*brokerAddr}
	//stressTest(brokerAddrs)
	//latencyTest(brokerAddrs)
	NormalTest(brokerAddrs)
}

//压力测试代码
func stressTest(addr []string) {
	fmt.Println("producerNum:", producerNum, " partitionNum:", partitionNum, " reCreateTopic", reCreateTopic, " msgLen:", msgLen)
	var wg sync.WaitGroup
	exitChan := make(chan bool)
	sendMsg := generateString(msgLen)
	for i := 0; i < producerNum; i++ {
		p, err := producer.NewProducer(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0 && reCreateTopic {
			p.DeleteTopic("fff") //先删除原来的分区
			//time.Sleep(1000 * time.Millisecond)
			p.CreatTopic("fff", int32(partitionNum)) //创建新的分区
		}
		wg.Add(1)
		go func() {
			producerHandle(p, exitChan, sendMsg)
			//atomic.AddInt64(&sum, times)
			wg.Done()
		}()
	}
	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号
	startTime := time.Now()
	startSum := sum
	timeTicker := time.NewTicker(time.Second) //每秒触发一次
	//atomic.StoreInt32(&sum, 0)
	myLogger.Logger.PrintfDebug("开始数据量： %d", startSum)
	lastSecendSum := sum
	for {
		select {
		case s := <-exitSignal: //退出信号来了
			myLogger.Logger.Print("exitSignal:", s)
			close(exitChan) //关闭退出管道，通知所有协程退出
			goto exit
		case <-timeTicker.C:
			curSum := atomic.LoadInt32(&sum)
			myLogger.Logger.PrintfDebug("发送速率: %d / s, 当前发送总量 %d", curSum-lastSecendSum, curSum)
			lastSecendSum = curSum
		}
	}
exit:
	wg.Wait() //等待退出

	endTime := time.Now()
	seconds := int64(endTime.Sub(startTime).Seconds())
	if seconds == 0 {
		return
	}
	myLogger.Logger.PrintfDebug("生产者数量: %d   平均发送速率: %d   发送总量: %d", producerNum, int64(sum-startSum)/seconds, sum)

}

//延迟测试代码
func latencyTest(addr []string) {
	fmt.Println("producerNum:", producerNum, " partitionNum:", partitionNum, " reCreateTopic", reCreateTopic, " msgLen:", msgLen)
	var wg sync.WaitGroup
	exitChan := make(chan bool)
	//sendMsg := generateString(msgLen)
	for i := 0; i < producerNum; i++ {
		p, err := producer.NewProducer(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0 && reCreateTopic {
			p.DeleteTopic("fff") //先删除原来的分区
			//time.Sleep(1000 * time.Millisecond)
			p.CreatTopic("fff", int32(partitionNum)) //创建新的分区
		}
		wg.Add(1)
		go func() {
			producerHandleLatencyTest(p, exitChan)
			//atomic.AddInt64(&sum, times)
			wg.Done()
		}()
	}
	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号

	s := <-exitSignal //退出信号来了
	myLogger.Logger.Print("exitSignal:", s)
	close(exitChan) //关闭退出管道，通知所有协程退出
	wg.Wait()       //等待退出

}

func NormalTest(addr []string) {
	fmt.Println("producerNum:", producerNum, " partitionNum:", partitionNum, " reCreateTopic", reCreateTopic)
	var wg sync.WaitGroup
	exitChan := make(chan bool)
	for i := 0; i < producerNum; i++ {
		p, err := producer.NewProducer(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if i == 0 && reCreateTopic {
			p.DeleteTopic("fff") //先删除原来的分区
			//time.Sleep(1000 * time.Millisecond)
			p.CreatTopic("fff", int32(partitionNum)) //创建新的分区
		}
		wg.Add(1)
		go func() {
			producerHandleSync(p, exitChan)
			wg.Done()
		}()

	}
	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill) //监听信号
	s := <-exitSignal                                //退出信号来了
	myLogger.Logger.Print("exitSignal:", s)
	close(exitChan) //关闭退出管道，通知所有协程退出
	wg.Wait()
}

//每个生产者的处理函数，不等的ack
func producerHandle(p *producer.Producer, exitChan <-chan bool, sendMsg string) {
	var i int64
	for {
		select {
		case _, ok := <-exitChan: //退出
			if !ok {
				p.Close()
				return
			}
		default:
			i++
			err := p.PubilshWithoutAck("fff", []byte(sendMsg), 0)
			//time.Sleep(1000 * time.Millisecond)
			if err != nil {
				myLogger.Logger.Print(err)
				i--
				continue
			}
			atomic.AddInt32(&sum, 1)
		}
	}
}

//每个生产者的处理函数，同步发送数据
func producerHandleSync(p *producer.Producer, exitChan <-chan bool) {
	var i int32 = 1
	for {
		select {
		case _, ok := <-exitChan:
			if !ok {
				return
			}
		default:
			s := fmt.Sprintf("消息%d", i)
			time.Sleep(1000 * time.Millisecond)
			err := p.Pubilsh("fff", []byte(s), i)
			if err != nil {
				myLogger.Logger.Print(err)
				//os.Exit(1)
				continue
			}
			i++
			//atomic.AddInt32(&sum, 1)
		}
	}
}

//每个生产者的处理函数，同步发送数据
func producerHandleLatencyTest(p *producer.Producer, exitChan <-chan bool) {
	var i int32 = 1
	for {
		select {
		case _, ok := <-exitChan:
			if !ok {
				return
			}
		default:
			time := strconv.FormatInt(time.Now().UnixNano(), 10) //当前时间
			err := p.Pubilsh("fff", []byte(time), 0)
			//sendTime, err := strconv.Atoi(string(time))
			//myLogger.Logger.PrintDebug2("Pubilsh ", time)
			//myLogger.Logger.PrintDebug2("Pubilsh sendTime ", sendTime)
			if err != nil {
				myLogger.Logger.Print(err)
				//os.Exit(1)
				continue
			}
			i++
			//atomic.AddInt32(&sum, 1)
		}

	}
}

//获取本机IP地址
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

//生成指定长度的字符串
func generateString(n int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, bytes[i%len(bytes)])
	}
	return string(result)
}
