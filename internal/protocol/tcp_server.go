package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

//tcp服务
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		//accept连接
		clientConn, err := listener.Accept()
		fmt.Println(clientConn)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				//这个其实是一个退避策略，防止这个协程一直占用着M（线程）
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		//连接分配给协程
		//这里用到wg,这里要结合main的stop方法中的wg.wait()来看
		//stop中的wg.wait()---->由于tcp协程和http协程可能还有连接 正在处理，等待处理完成
		//那么这个地方作用是: accept报错后(stop中的监听关闭后 再accept必报错),需要让之前正在进行处理的请求处理完
		//大概就是，整个进程等tcp服务结束，tcp服务等handle协程结束
		//(为啥http那边不需要等handle协程结束呢?)
		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
