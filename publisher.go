package autofillcopyfield

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Publish 发布消息
func Publish(event Event) (response <-chan []byte, err error) {
	b, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	msg := message.Message{
		UUID:    watermill.NewULID(),
		Payload: b,
	}
	container := GetContainer()
	err = container.publisher.Publish(event.Topic, &msg)
	return nil, err
}
