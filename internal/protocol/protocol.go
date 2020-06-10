package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4
	//BigEndian 是指 按照从低地址到高地址的顺序存放数据的高位字节到低位字节
	binary.BigEndian.PutUint32(beBuf, size)
	//写总长度  4 + data长度
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	//再写frameType
	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	//再写总长度
	//（这里的写都是写到tcp缓存内）
	n, err = w.Write(data)
	return n + 8, err
}
