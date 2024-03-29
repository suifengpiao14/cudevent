package cudevent

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var DEBUG = false
var topic = "cudevent_channel"

// pubSub 事件发布器，使用最简单的go channel发布订阅作为解耦业务和通信，如需发布到其它消息队列可专注从go channel中获取消息转发，无需耦合业务
// pubSub 固定不可变的go channel
var pubSub = gochannel.NewGoChannel(
	gochannel.Config{},
	watermill.NewStdLogger(DEBUG, DEBUG),
	//watermill.NopLogger{},
)

// publish 发布消息
func publish(payload *_ChangedMessage) (err error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := message.Message{
		UUID:    watermill.NewULID(),
		Payload: b,
	}
	err = pubSub.Publish(topic, &msg)
	return err
}

// Subscriber 增加订阅者
func Subscriber(ctx context.Context, fn func(msg *message.Message) (err error)) (err error) {
	messageChan, err := pubSub.Subscribe(context.Background(), topic)
	if err != nil {
		return err
	}
	go func() {
		for msg := range messageChan {
			err = fn(msg)
			if err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}()
	return nil
}
