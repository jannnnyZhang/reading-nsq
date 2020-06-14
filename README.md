nsq v1.2.1 中文注解进行中

nsqlookupd源码开跑

cd apps/nsqlookupd && go run main.go

nsqd源码开跑

cd apps/nsqd && go run main.go options.go --lookupd-tcp-address=127.0.0.1:4160
