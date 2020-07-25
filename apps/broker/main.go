package main

import (
	"../../broker"
	"fmt"
)

func main() {
	fmt.Println("Hello, World!")

	b, err:= broker.New()
	if err != nil {
		fmt.Println(err)
	}
	b.Run()
	fmt.Println("main bye")
}
