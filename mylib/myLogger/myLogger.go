package myLogger

import (
	"fmt"
	"log"
	"os"
	"time"
)

type MyLogger struct {
	loger *log.Logger
}

var Logger *MyLogger
func New(logDir string) (*MyLogger, error) {

	err := os.Mkdir(logDir, os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		fmt.Printf("mkdir success!\n")
	}

	logFile, err := os.Create(logDir + time.Now().Format("20060102")  + ".log");
	if err != nil {
		fmt.Println(err);
	}

	loger := log.New(logFile, "broker", log.Ldate|log.Ltime|log.Lshortfile);
	Logger = &MyLogger{loger: loger}
	return Logger, err
}

func (l *MyLogger) Printf(format string, v ...interface{}) {
	fmt.Print("log:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.loger.Output(2, fmt.Sprintf(format, v...))
}

func (l *MyLogger) Print(v ...interface{}) {
	fmt.Print("log:")
	fmt.Print(v...)
	fmt.Println()
	l.loger.Output(2, fmt.Sprint(v...))
}


// Fatal is equivalent to l.Print() followed by a call to os.Exit(1).
func (l *MyLogger) Fatal(v ...interface{}) {
	l.loger.Fatal(v)
	os.Exit(1)
}