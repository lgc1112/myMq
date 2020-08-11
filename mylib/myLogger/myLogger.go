package myLogger

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type MyLogger struct {
	sync.RWMutex
	infoLogger    *log.Logger
	errorLogger   *log.Logger
	debugLogger   *log.Logger
	warningLogger *log.Logger
}

var Logger *MyLogger

func New(logDir string) (*MyLogger, error) {

	err := os.MkdirAll(logDir+"info/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}

	err = os.MkdirAll(logDir+"error/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}

	err = os.MkdirAll(logDir+"debug/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}

	err = os.MkdirAll(logDir+"warning/", os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}

	infoLoggerFile, err := os.Create(logDir + "info/" + time.Now().Format("20060102") + ".log")
	if err != nil {
		fmt.Println(err)
	}
	errorLoggerFile, err := os.Create(logDir + "error/" + time.Now().Format("20060102") + ".log")
	if err != nil {
		fmt.Println(err)
	}
	debugLoggerFile, err := os.Create(logDir + "debug/" + time.Now().Format("20060102") + ".log")
	if err != nil {
		fmt.Println(err)
	}
	warningLoggerFile, err := os.Create(logDir + "warning/" + time.Now().Format("20060102") + ".log")
	if err != nil {
		fmt.Println(err)
	}

	infoLogger := log.New(infoLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger := log.New(errorLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	debugLogger := log.New(debugLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	warningLogger := log.New(warningLoggerFile, "broker", log.Ldate|log.Ltime|log.Lshortfile)
	Logger = &MyLogger{
		infoLogger:    infoLogger,
		errorLogger:   errorLogger,
		debugLogger:   debugLogger,
		warningLogger: warningLogger,
	}
	return Logger, err
}

func (l *MyLogger) Print(v ...interface{}) {
	//l.Lock()
	//defer l.Unlock()
	//fmt.Print("info:")
	//fmt.Print(v...)
	//fmt.Println()
	//l.infoLogger.Output(2, fmt.Sprint(v...))
}

func (l *MyLogger) Printf(format string, v ...interface{}) {
	//l.Lock()
	//defer l.Unlock()
	//fmt.Print("info:")
	//fmt.Printf(format, v...)
	//fmt.Println()
	//l.infoLogger.Output(2, fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintfError(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("error:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.errorLogger.Output(2, "error:"+fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintError(v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("error:")
	fmt.Print(v...)
	fmt.Println()
	l.errorLogger.Output(2, "error:"+fmt.Sprint(v...))
}

func (l *MyLogger) PrintfDebug(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Debug:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.debugLogger.Output(2, "Debug:"+fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintDebug(v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Debug:")
	fmt.Print(v...)
	fmt.Println()
	l.debugLogger.Output(2, "Debug:"+fmt.Sprint(v...))
}

func (l *MyLogger) PrintfDebug2(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Debug:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.debugLogger.Output(2, "Debug:"+fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintDebug2(v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Debug:")
	fmt.Print(v...)
	fmt.Println()
	l.debugLogger.Output(2, "Debug:"+fmt.Sprint(v...))
}

func (l *MyLogger) PrintfWarning(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Warning:")
	fmt.Printf(format, v...)
	fmt.Println()
	l.warningLogger.Output(2, "Warning:"+fmt.Sprintf(format, v...))
}

func (l *MyLogger) PrintWarning(v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	fmt.Print("Warning:")
	fmt.Print(v...)
	fmt.Println()
	l.warningLogger.Output(2, "Warning:"+fmt.Sprint(v...))
}

// Fatal is equivalent to l.Print() followed by a call to os.Exit(1).
func (l *MyLogger) Fatal(v ...interface{}) {
	l.errorLogger.Fatal(v)
	os.Exit(1)
}
