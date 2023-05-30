package syncdata

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// GetPublisherFn 获取发布器函数
type GetPublisherFn func() (publisher message.Publisher, err error)

func MemoryPublisherFn (){
	
}

// Publish 发布消息
func Publish(publisher message.Publisher, topic string, payload *ChangedPayload) (err error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := message.Message{
		UUID:    watermill.NewULID(),
		Payload: b,
	}
	err = publisher.Publish(topic, &msg)
	return err
}
