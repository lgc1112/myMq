package myLogger

import (
	"fmt"
	"log"
	"os"
	"time"
)

type MyLogger struct {
	infoLogger *log.Logger
	errorLogger *log.Logger
}

var Logger *MyLogger
func New(logDir string) (*MyLogger, error) {

	err := os.MkdirAll(logDir + "info/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}

	err = os.MkdirAll(logDir + "error/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}
	infoLoggerFile, err := os.Create(logDir + "info/" + time.Now().Format("20060102")  + ".log")
	if err != nil {
		fmt.Println(err)
	}
	errorLoggerFile, err := os.Create(logDir + "error/" + time.Now().Format("20060102")  + ".log")
	if err != nil {
		fmt.Println(err)
	}

	infoLogger := log.New(infoLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger := log.New(errorLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	Logger = &MyLogger{
		infoLogger: infoLogger,
		errorLogger: errorLogger,
	}
	return Logger, err
}

func (l *MyLogger) Print(v ...interface{}) {
	fmt.Print("info:")
	fmt.Print(v...)
	fmt.Println()
	l.infoLogger.Output(2, fmt.Sprint(v...))
}

func (l *MyLogger) Printf(format string, v ...interface{}) {
	fmt.Print("info:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.infoLogger.Output(2, fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintfError(format string, v ...interface{}) {
	fmt.Print("error:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.errorLogger.Output(2, "error:" + fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintError(v ...interface{}) {
	fmt.Print("error:")
	fmt.Print(v...)
	fmt.Println()
	l.errorLogger.Output(2, "error:" + fmt.Sprint(v...))
}


// Fatal is equivalent to l.Print() followed by a call to os.Exit(1).
func (l *MyLogger) Fatal(v ...interface{}) {
	l.errorLogger.Fatal(v)
	os.Exit(1)
}