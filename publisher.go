package syncdata

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Publish 发布消息
func Publish(topic string, payload ChangedPayload) (err error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := message.Message{
		UUID:    watermill.NewULID(),
		Payload: b,
	}
	container := GetContainer()
	err = container.publisher.Publish(topic, &msg)
	return err
}
