package main

import (
	"fmt"
	"go_dev/log_agent/conf"
	"go_dev/log_agent/etcd"
	"go_dev/log_agent/kafka"
	"go_dev/log_agent/tail_log"
	"go_dev/log_agent/utils"
	"gopkg.in/ini.v1"
	sync2 "sync"
	"time"
)

var cfg = new(conf.AppConf)

//func run() {
//	// 1.读取日志
//	for {
//		select {
//		case line := <-tail_log.ReadChan():
//			// 2.发送到kafka
//			kafka.Send(cfg.KafkaConf.Topic, line.Text)
//		default:
//			time.Sleep(time.Second)
//		}
//	}
//}

// logAgent 入口程序
func main() {
	// 0.加载配置文件
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Println("load ini failed err:", err)
		return
	}
	fmt.Println("ini config success")

	// 1.初始化kafka
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Println("init kafka failed:", err)
	}
	fmt.Println("Init kafka success")

	// 1.加载Etcd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("etcd ini failed err:", err)
		return
	}
	fmt.Println("ini etcd success")
	// 实现每个logAgent都拉取自己独有的配置，用ip做区分
	ipStr, err := utils.GetOutBoundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Println("etcd.GetConf failed,err:", err)
		return
	}
	fmt.Println("get conf from etcd success:", logEntryConf)
	// 2.2 派一个哨兵去监视日志收集项的变化（有变化及时通知我的logAgent实现热加载配置）

	for key, value := range logEntryConf {
		fmt.Println("key:", key, "value:", value)
	}

	// 3.收集日志发往kafka
	tail_log.Init(logEntryConf)
	newConfChan := tail_log.NewConfChan() // 从taillog包获取对外暴露的通道
	var sync sync2.WaitGroup
	sync.Add(1)
	go etcd.WatchConf(etcdConfKey, newConfChan) // 哨兵发现最新的配置信息会通知上面的通道
	sync.Wait()
	//run()
}
