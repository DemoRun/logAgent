package tail_log

import (
	"fmt"
	"github.com/hpcloud/tail"
	"go_dev/log_agent/kafka"
	"golang.org/x/net/context"
)

var (
	tailObj *tail.Tail
	//LogChan chan string
)

// 一个日志收集的任务
type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	// 实现退出t.run()
	ctx         context.Context
	canncelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:        path,
		topic:       topic,
		ctx:         ctx,
		canncelFunc: cancel,
	}
	tailObj.init() // 根据路径去打开对应的日志信息
	return
}

func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件哪个位置开始读
		MustExist: false,                                // 文件不存在报错
		Poll:      true,                                 //
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file err:", err)
	}
	// 当goroutine执行函数退出的时候，goroutine就结束了
	go t.run() // 直接去采集日志发送到kafka
}

func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task:%s_%s 结束了...\n", t.path, t.topic)
			return
		case line := <-t.instance.Lines: // 从tailObj通道中读取数据
			//kafka.SendToKafka(t.topic, line.Text) // 函数掉函数
			// 先把日志发到一个通道中
			kafka.SenToChan(t.topic, line.Text)
			// kafka从那个包中有单独的goroutine去取日志数据发送到kafka
		}
	}
}

//func (t *TailTask) ReadChan() <-chan *tail.Line {
//	return t.instance.Lines
//
//}
