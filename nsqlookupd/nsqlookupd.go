package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options //配置参数
	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *tcpServer //tcp服务
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB //注册DB
}

func New(opts *Options) (*NSQLookupd, error) {
	var err error

	//日志组件
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	//打印版本
	l.logf(LOG_INFO, version.String("nsqlookupd"))

	//tcp监听
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}

	//tcp监听
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
func (l *NSQLookupd) Main() error {
	ctx := &Context{l}

	exitCh := make(chan error)
	var once sync.Once

	//err处理函数
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}
	//tcp服务
	l.tcpServer = &tcpServer{ctx: ctx}

	/**
		Wrap会创建一个协程，
		创建的协程执行exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf)),阻塞至产生err
		此时执行 exitFunc(err) 会发送err至exitCh
	 */
	l.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf))
	})
	//http服务
	httpServer := newHTTPServer(ctx)

	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	err := <-exitCh
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	//关闭tcp监听
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	//断掉所有tcp连接(文件描述符销毁等)
	if l.tcpServer != nil {
		l.tcpServer.CloseAll()
	}

	//如果有http监听，则关闭
	if l.httpListener != nil {
		l.httpListener.Close()
	}
	//由于tcp协程和http协程可能还有连接 正在处理，等待处理完成
	l.waitGroup.Wait()
}
