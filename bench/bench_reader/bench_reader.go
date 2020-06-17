package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

//测试读性能
//整体代码架构和bench_write.go非常相似，可以参考bench_writer.go的代码注解
var (
	runfor     = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	size       = flag.Int("size", 200, "size of messages")
	topic      = flag.String("topic", "sub_bench", "topic to receive messages on")
	channel    = flag.String("channel", "ch", "channel to receive messages on")
	deadline   = flag.String("deadline", "", "deadline to start the benchmark run")
	rdy        = flag.Int("rdy", 2500, "RDY count to use")
)

var totalMsgCount int64 //总读取条数

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_reader] ")

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0) //这个返回的是最大P数量
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*runfor, workers, *tcpAddress, *topic, *channel, rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024, //吞吐量
		float64(tmc)/duration.Seconds(),	//OPS
		float64(duration/time.Microsecond)/float64(tmc))	//
}

func subWorker(td time.Duration, workers int, tcpAddr string, topic string, channel string, rdyChan chan int, goChan chan int, id int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw) //身份认证
	nsq.Subscribe(topic, channel).WriteTo(rw) //订阅
	rdyChan <- 1
	<-goChan
	nsq.Ready(*rdy).WriteTo(rw) //一次接受条数
	rw.Flush() //发送tcp缓冲区
	//这里两次的接收是身份认证和订阅返回的消息
	nsq.ReadResponse(rw)
	nsq.ReadResponse(rw)
	var msgCount int64
	go func() {
		time.Sleep(td) //这里开了一个定时任务，到点关闭
		conn.Close()
	}()
	for {
		//从缓冲区读取一条
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp) //解包
		if err != nil {
			panic(err.Error())
		}
		if frameType == nsq.FrameTypeError {
			panic(string(data))
		} else if frameType == nsq.FrameTypeResponse { //正常的消息响应返回是这个
			continue
		}
		msg, err := nsq.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		nsq.Finish(msg.ID).WriteTo(rw)
		msgCount++
		//这里是flush频率，这里没看懂，为啥超过0.75就是每条刷一次呢？
		if float64(msgCount%int64(*rdy)) > float64(*rdy)*0.75 {
			rw.Flush()
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
