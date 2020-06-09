package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqlookupd"
)

func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	//实例化一个集合
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	return flagSet
}

type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	//用了svc包，Run启动，
	//首先会调Init，再调Start, Start是启动核心，Init做一些初始化
	//发送SIGINT，SIGTERM信号会回调Stop方法
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	//这个判断是否有必要？svc源码已经做了相应判断
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	//获取默认配置
	opts := nsqlookupd.NewOptions()

	flagSet := nsqlookupdFlagSet(opts)
	//解析除了第一个参数
	flagSet.Parse(os.Args[1:])

	//获取是否参数有version，如果有则输入nsqd版本号
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	//如果有指定配置文件，则会读取配置配置文件
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}

	//解析配置
	//优先级
	//1.命令行参数
	//2.配置文件
	//3.默认参数
	options.Resolve(opts, flagSet, cfg)
	//实例化nsqLookupd
	nsqlookupd, err := nsqlookupd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	//赋值
	p.nsqlookupd = nsqlookupd

	//由于start不能够被阻塞，所以这里需要开一个协程
	go func() {
		//nsqlookupd主逻辑，阻塞
		err := p.nsqlookupd.Main()
		//如果有err,则执行stop方法,并且终止
		if err != nil {
			p.Stop()
			//退出
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	//用了once锁，只exit一次
	p.once.Do(func() {
		p.nsqlookupd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}
