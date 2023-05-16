package autofillcopyfield

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	// For this example, we're using just a simple logger implementation,
	// You probably want to ship your own implementation of `watermill.LoggerAdapter`.
	logger = watermill.NewStdLogger(false, false)
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
	switch event.Type {
	case EVENT_TYPE_CREATED, EVENT_TYPE_UPDATED, EVENT_TYPE_DELETED:
		//异步
		err = container.publisher.Publish(event.Topic, &msg)
		return nil, err
	case EVENT_TYPE_CREATING, EVENT_TYPE_UPDATING, EVENT_TYPE_DELETING:
		//同步
		router, err := message.NewRouter(message.RouterConfig{}, logger)
		if err != nil {
			return nil, err
		}
		incomingTopic := event.Topic
		outgoingTopic := fmt.Sprintf("%s_response", incomingTopic)
		router.AddHandler(
			event.EventID, // handler name, must be unique
			incomingTopic, // topic from which we will read events
			container.subscriber,
			outgoingTopic, // topic to which we will publish events
			container.publisher,
			message.PassthroughHandler,
		)

		ctx := context.Background()
		if err := router.Run(ctx); err != nil {
			panic(err)
		}
		return nil, err
	}
	err = errors.Errorf("unexpected event.Type,got:%s", event.Type)
	return nil, err
}

type structHandler struct {
	// we can add some dependencies here
}

func (s structHandler) Handler(msg *message.Message) ([]*message.Message, error) {
	log.Println("structHandler received message", msg.UUID)
	msg = message.NewMessage(watermill.NewUUID(), []byte("message produced by structHandler"))
	return message.Messages{msg}, nil
}
