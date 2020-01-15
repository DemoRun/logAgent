package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var client *clientv3.Client

// 需要收集的日志配置信息
type LogEntry struct {
	Path  string `json:"path"`  // 日志存放的路径
	Topic string `json:"topic"` // 日志要发往kafka的topic
}

func Init(addr string, timeOut time.Duration) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeOut,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

// 从etcd中跟进key获取配置项
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}

	for _, ev := range resp.Kvs {
		//fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Println("json Unmarshal failed:", err)
			return
		}
	}
	return
}

// etcd watch
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	ch := client.Watch(context.Background(), key) // <-chan WatchResponse
	for wresp := range ch {
		for _, evt := range wresp.Events {
			fmt.Printf("Type: %s Key:%s Value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
			// 通知tskMsg
			// 先判断类型
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("unmarshal failed:", err)
					continue
				}
			}
			fmt.Println("get new conf :", newConf)
			newConfCh <- newConf
		}
	}
}
