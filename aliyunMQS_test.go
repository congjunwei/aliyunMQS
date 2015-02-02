package aliyunMQS

import (
	//"errors"
	. "github.com/smartystreets/goconvey/convey"
	//"log"
	"testing"
)

var accessKey string = "*"
var accessSecret string = "*"
var queueOwnId string = "*"
var mqsUrl string = "mqs-cn-beijing.aliyuncs.com"

func TestQueue(t *testing.T) {
	Convey("队列接口测试", t, func() {
		var queue Queue
		queue.NewMQS(accessKey, accessSecret, queueOwnId, mqsUrl)

		queuename := "test"
		Convey("创建队列", func() {
			param := map[string]int{"DelaySeconds": 1}
			_, err := queue.CreateQueue(queuename, param)
			So(err, ShouldBeNil)
			//	So(err, ShouldEqual, errors.New("204 No Content"))
			//	So(err, ShouldEqual, errors.New("409 Conflict"))
		})

		Convey("设置队列属性", func() {
			param := map[string]int{"DelaySeconds": 6}
			_, err := queue.SetQueueAttributes(queuename, param)
			So(err, ShouldBeNil)
		})
		Convey("获取队列属性", func() {
			_, err := queue.GetQueueAttributes(queuename)
			So(err, ShouldBeNil)
		})
		Convey("获取队列列表", func() {
			_, err := queue.ListQueue("", "", "")
			So(err, ShouldBeNil)
		})
		Convey("删除队列", func() {
			_, err := queue.DeleteQueue(queuename)
			So(err, ShouldBeNil)
		})
	})
}

func TestMessage(t *testing.T) {
	Convey("消息接口测试", t, func() {
		var msg Message
		msg.NewMQS(accessKey, accessSecret, queueOwnId, mqsUrl)
		queuename := "test"
		Convey("发送消息到指定的消息队列", func() {
			messagebody := "hahah111"
			param := map[string]int{"DelaySeconds": 1}
			_, err := msg.SendMessage(queuename, messagebody, param)
			So(err, ShouldBeNil)
		})

		Convey("用于消费者消费消息队列的消息", func() {
			_, err := msg.ReceiveMessage(queuename, 1)
			So(err, ShouldBeNil)
			//t.Logf("content:%s", content)
		})

		Convey("用于删除已经被消费过的消息", func() {
			_, err := msg.DeleteMessage(queuename, "")
			So(err, ShouldBeNil)
		})
		Convey("用于消费者查看消息", func() {
			_, err := msg.PeekMessage(queuename)
			So(err, ShouldBeNil)
		})
	})
}
