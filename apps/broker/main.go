package main

import (
	"../../broker"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	fmt.Println("broker start")
	exitSignal := make(chan os.Signal)
	signal.Notify(exitSignal, os.Interrupt, os.Kill)//监听信号
	b, err:= broker.New(exitSignal) //新建broker实例
	if err != nil {
		fmt.Println(err)
	}
	b.Run() //运行broker实例
	fmt.Println("broker stop")
}
