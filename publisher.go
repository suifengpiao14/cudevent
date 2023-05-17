package syncdata

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Publish 发布消息
func Publish(event Event) (err error) {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := message.Message{
		UUID:    watermill.NewULID(),
		Payload: b,
	}
	container := GetContainer()
	err = container.publisher.Publish(event.Topic, &msg)
	return err
}
