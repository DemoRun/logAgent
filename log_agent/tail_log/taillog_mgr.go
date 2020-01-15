package tail_log

import (
	"fmt"
	"go_dev/log_agent/etcd"
	"time"
)

var tskMgr *taillogMgr

// tailTsk 管理者
type taillogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &taillogMgr{
		logEntry:    logEntryConf, // 把当前的日志收集配置信息保存起来
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区的通道
	}
	for _, logEntry := range logEntryConf {
		// 将所有配置记录到tskMap中
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mk] = tailObj
	}
	go tskMgr.run()
}

// 监听自己的newConfChan，有了新配置过来做对应处理
func (t *taillogMgr) run() {
	for {
		select {
		case NewConf := <-t.newConfChan:
			for _, conf := range NewConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[mk]
				if ok {
					// 对比一致，不需要修改
					continue
				} else {
					// 新增
					tailObj := NewTailTask(conf.Topic, conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			for _, c1 := range t.logEntry {
				isDelete := true
				for _, c2 := range NewConf {
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// 把c1对应的这个tailObj停掉
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[mk].canncelFunc()
				}
			}
			fmt.Println("新的配置来了：", NewConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// 向外暴露tskMgr的newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
