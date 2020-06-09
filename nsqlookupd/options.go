package nsqlookupd

import (
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	BroadcastAddress string `flag:"broadcast-address"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	//默认配置
	return &Options{
		LogPrefix:        "[nsqlookupd] ", //日志前缀
		LogLevel:         lg.INFO, //日志等级
		TCPAddress:       "0.0.0.0:4160", //默认tcp监听端口
		HTTPAddress:      "0.0.0.0:4161", //默认http监听端口
		BroadcastAddress: hostname, //broadcast地址

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
