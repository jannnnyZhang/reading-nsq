package nsqlookupd

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx   *Context
	conns sync.Map
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	//这个是读协议类型，通信时前4个Byte表明协议类型
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	//如果没读到 则报错
	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)
	fmt.Println(protocolMagic)
	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	//目前只有  V1 协议类型
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{ctx: p.ctx}
	default:
		protocol.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	//把连接信息储存
	//sync.Map可以保证并发安全
	p.conns.Store(clientConn.RemoteAddr(), clientConn)
	//循环，这个是真正的处理逻辑
	err = prot.IOLoop(clientConn)

	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
	}
	//连接断开，需要删除对应连接信息
	p.conns.Delete(clientConn.RemoteAddr())
}

//关闭所有连接
func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}
