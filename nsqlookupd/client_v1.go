package nsqlookupd

import (
	"net"
)

type ClientV1 struct {
	net.Conn //连接资源
	peerInfo *PeerInfo //身份信息
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		Conn: conn,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}
