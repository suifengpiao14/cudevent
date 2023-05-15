package autofillcopyfield

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/suifengpiao14/logchan/v2"
)

type LogName string

func (l LogName) String() string {
	return string(l)
}

const LOG_INFO_SUBSCRIBER LogName = "LOG_INFO_SUBSCRIBER"

type SubscriberLogInfo struct {
	Msg   message.Message
	Event Event
	err   error
}

func (l SubscriberLogInfo) GetName() logchan.LogName {
	return LOG_INFO_SUBSCRIBER
}

func (l SubscriberLogInfo) Error() error {
	return l.err
}

type SubscriberFn func(event *Event) (err error)

func Subscriber(topc string, fn SubscriberFn) (err error) {
	messageChan, err := defaultContainer.subscriber.Subscribe(context.Background(), topc)
	if err != nil {
		return err
	}
	go func() {
		for msg := range messageChan {
			msg.Ack()
			process(msg, fn)

		}
	}()
	return nil

}

func process(msg *message.Message, handlerFn SubscriberFn) {
	var err error
	event := Event{}
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
	err = handlerFn(&event)
}
