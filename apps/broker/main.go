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
	b, err:= broker.New(exitSignal)
	if err != nil {
		fmt.Println(err)
	}
	b.Run()
	fmt.Println("broker stop")
}
