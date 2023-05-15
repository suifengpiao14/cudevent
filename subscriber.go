package autofillcopyfield

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/suifengpiao14/logchan/v2"
)

var ERR_HANDLER_NOT_FOUND = errors.New("handler not found")

type LogName string

func (l LogName) String() string {
	return string(l)
}

const LOG_INFO_SUBSCRIBER LogName = "LOG_INFO_SUBSCRIBER"

type SubscriberLogInfo struct {
	Msg   message.Message
	Event EventWithSub
	err   error
}

func (l SubscriberLogInfo) GetName() logchan.LogName {
	return LOG_INFO_SUBSCRIBER
}

func (l SubscriberLogInfo) Error() error {
	return l.err
}

func Subscriber() (err error) {
	container := GetContainer()
	messages, err := container.subscriber.Subscribe(context.Background(), container.topic)
	if err != nil {
		return err
	}
	for msg := range messages {
		msg.Ack()
		process(msg)

	}
	return nil

}

func process(msg *message.Message) {
	var err error
	event := EventWithSub{}
	defer func() {
		logInfo := SubscriberLogInfo{
			Msg:   *msg,
			Event: event,
			err:   err,
		}
		logchan.SendLogInfo(logInfo)
	}()
	err = json.Unmarshal(msg.Payload, &event)
	if err != nil {
		return
	}
	container := GetContainer()
	err = container.Work(event)
}
