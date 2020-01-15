package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

//专门往kafka里边写日志模块
type logData struct {
	topic string
	data  string
}

var (
	client      sarama.SyncProducer // 生成一个全局连接kafka的生产者client
	logDataChan chan *logData
)

// Init初始化client
func Init(addrs []string, maxSize int) (err error) {
	config := sarama.NewConfig()
	// tail包使用
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送数据ACK模式0,1,all
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区选择方式（指定,hash,轮询）
	config.Producer.Return.Successes = true                   // 成功交付信息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("producer close, err:", err)
		return
	}
	// 初始化logDataChan
	logDataChan = make(chan *logData, maxSize)
	// 开启后台的goroutine从通道中取数据发往kafka
	go sendToKafka()
	return
}

// 给外部暴露的一个函数，该函数只是把日志数据发送到一个内部的channel中
func SenToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- msg
}

// 真正往kafka发送日志的函数
func sendToKafka() {
	for {
		select {
		case ld := <-logDataChan:
			msg := &sarama.ProducerMessage{}
			msg.Topic = ld.topic
			msg.Value = sarama.StringEncoder(ld.data)

			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send message failed,", err)
				return
			}
			fmt.Printf("pid:%v offset:%v\n", pid, offset)
		default:
			time.Sleep(time.Microsecond * 50)
		}
	}

}
