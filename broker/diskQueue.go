package broker

import (
	"../mylib/myLogger"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)
const MaxBytesPerFile = 100 * 1024 * 1024
type diskQueue struct {
	rOffset int64 //文件中的偏移
	wOffset int64
	rFileNum int64 //文件编号
	wFileNum int64
	msgNum int64 //消息数量

	path string
	fileName string
	readyChan chan []byte
	writeChan chan []byte
	writeFinished chan error
	exitChan chan string

	maxBytesPerFile int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	//writer    *bufio.Writer
}

func NewDiskQueue(path string) *diskQueue{
	d := &diskQueue{
		path: path,
		readyChan: make(chan []byte),
		writeChan: make(chan []byte),
		exitChan: make(chan string),
		writeFinished: make(chan error),
		maxBytesPerFile: MaxBytesPerFile,
		fileName : path + "disk.data",
	}
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		fmt.Printf("mkdir failed![%v]\n", err)
	} else {
		//fmt.Printf("mk log dir success!\n")
	}
	d.retrieveDiskMetaData()//恢复磁盘中的数据
	go d.ioLoop()
	return d
}


func (d *diskQueue) exit() error {
	err := d.sync()//保存数据
	close(d.readyChan)
	close(d.writeChan)
	close(d.exitChan)
	close(d.writeFinished)
	return err
}

func (d *diskQueue)ioLoop()  {
	var readyData []byte
	var err error
	var readyChan chan []byte
	for {
		if d.msgNum > 0 { //磁盘中有数据可读
			if readyData == nil {//没有要发送的数据，可以读新的数据发送了
				readyData, err = d.readDiskMsg()
				if err != nil {
					myLogger.Logger.PrintError("readDiskMsg OpenFile Error:", err)
				}
			}
		}
		if readyData != nil{
			readyChan = d.readyChan//有数据要发，可以发到管道
		}else {
			readyChan = nil //无数据，让其阻塞
		}
		select {
		case <- d.exitChan:
			goto exit
		case readyChan <- readyData: //尝试将数据发送到readChan
			readyData = nil //发送完毕
		case dataWrite := <-d.writeChan: //有数据要写磁盘
			d.writeFinished <- d.writeDiskMsg(dataWrite)  //写磁盘并通知
		}
	}
	exit:
		err = d.exit()
		if err != nil{
			myLogger.Logger.PrintError(err)
		}
		myLogger.Logger.Print("diskQueue ioLoop exit")

}

func (d *diskQueue) storeData(data []byte) error{
	d.writeChan <- data
	return <-d.writeFinished
}

func (d *diskQueue)readDiskMsg() ([]byte, error) {
	var err error
	var size int32

	if d.readFile == nil {
		rFileName := d.fileName + strconv.FormatInt(d.rFileNum,10)
		d.readFile, err = os.OpenFile(rFileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			myLogger.Logger.PrintError("readDiskMsg OpenFile Error:", err)
			return nil, err
		}
		myLogger.Logger.Print("readDiskMsg frome file: ", rFileName)

		if d.rOffset > 0 {
			_, err = d.readFile.Seek(d.rOffset, 0)//调到读偏移
			if err != nil {
				myLogger.Logger.PrintError("readDiskMsg Seek Error:", err)
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &size)//读长度
	if err != nil {
		myLogger.Logger.PrintError("readDiskMsg Read Error:", err)
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, size)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		myLogger.Logger.PrintError("readDiskMsg Read Error:", err)
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	d.msgNum -= 1
	d.rOffset = d.rOffset + int64(4 + size) //读偏移增加

	if d.msgNum == 0{//所有消息读完了，全部归0
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		deleteFileName := d.fileName + strconv.FormatInt(d.rFileNum, 10) //删除最后一个文件

		d.msgNum = 0 //全部归0
		d.wFileNum = 0
		d.rFileNum = 0
		d.wOffset = 0
		d.rOffset = 0

		d.persistDiskMetaData() //删除前先同步，保存好偏移数据，已防止访问已删除的数据
		err := os.Remove(deleteFileName)
		if err != nil {
			myLogger.Logger.PrintError("Remove file Error", deleteFileName, err)
		}else{
			myLogger.Logger.Print("remove file success:", deleteFileName)
		}
	}else{
		if d.rOffset > d.maxBytesPerFile {//这个文件已经读完了
			if d.readFile != nil {
				d.readFile.Close()
				d.readFile = nil
			}
			deleteFileName := d.fileName + strconv.FormatInt(d.rFileNum, 10)
			d.rFileNum++
			d.rOffset = 0
			d.persistDiskMetaData() //删除前先同步，保存好偏移数据，已防止访问已删除的数据
			err := os.Remove(deleteFileName)
			if err != nil {
				myLogger.Logger.PrintError("Remove file Error")
			}
			myLogger.Logger.Print("remove file success:", deleteFileName)
		}
	}

	return readBuf, nil
}

func (d *diskQueue)writeDiskMsg(data []byte) error {
	var err error
	if d.writeFile == nil {
		wFileName := d.fileName + strconv.FormatInt(d.wFileNum, 10)
		d.writeFile, err = os.OpenFile(wFileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			myLogger.Logger.PrintError("writeDiskMsg OpenFile Error:", err)
			return err
		}
		myLogger.Logger.Print("writeDiskMsg to file: ", wFileName)

		if d.wOffset > 0 {
			_, err = d.writeFile.Seek(d.wOffset, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
		//d.writer = bufio.NewWriter(d.writeFile)
	}

	//var buf [4]byte
	//bufs := buf[:]
	//binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	//d.writer.Write(bufs)
	//d.writer.Write(data)
	//d.writer.Flush()
	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian,  int32(len(data)))
	if err != nil {
		myLogger.Logger.PrintError("writeDiskMsg Write Error:", err)
		return err
	}

	_, err = buf.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("writeDiskMsg Write Error:", err)
		return err
	}

	_, err = d.writeFile.Write(buf.Bytes())
	if err != nil {
		myLogger.Logger.PrintError("writeDiskMsg Write Error:", err)
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.wOffset += int64(buf.Len())
	d.msgNum += 1
	if d.wOffset > d.maxBytesPerFile{ //已经超过最大文件大小，不能写下一条消息了
		d.wFileNum++
		d.wOffset = 0

		err = d.sync() //原文件sync到磁盘
		if err != nil {
			myLogger.Logger.PrintError(err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}


	return err
}

func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistDiskMetaData()
	if err != nil {
		return err
	}

	return nil
}


func (d *diskQueue) retrieveDiskMetaData() error {
	f, err := os.OpenFile(d.fileName, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		if os.IsNotExist(err){
			//_, err := os.Create(d.fileName)
			//myLogger.Logger.Print("creat file:", d.fileName)
			//if err != nil {
			//	myLogger.Logger.PrintError(err)
			//}
		}else{
			myLogger.Logger.PrintError(err)
		}
		return err
	}

	_, err = fmt.Fscanf(f, "%d %d %d %d %d\n",
		&d.rFileNum, &d.rOffset,
		&d.wFileNum, &d.wOffset, &d.msgNum)
	if err != nil {
		fmt.Println(err)
	}
	myLogger.Logger.Printf("read disData: %d %d %d %d %d ",
		d.rFileNum, d.rOffset,
		d.wFileNum, d.wOffset, d.msgNum)
	return nil
}


func (d *diskQueue) persistDiskMetaData() error {
	f, err := os.OpenFile(d.fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer f.Close()
	if err != nil {
		myLogger.Logger.PrintError(err)
	}

	_, err = fmt.Fprintf(f, "%d %d %d %d %d\n",
		d.rFileNum, d.rOffset,
		d.wFileNum, d.wOffset, d.msgNum)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}

	myLogger.Logger.Printf("persist disData: %d %d %d %d %d",
		d.rFileNum, d.rOffset,
		d.wFileNum, d.wOffset, d.msgNum)
	return nil
}