package syncdata

import (
	"context"

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

type SubscriberFn func(msg *message.Message) (err error)

func Subscriber(topc string, fn SubscriberFn) (err error) {
	messageChan, err := defaultContainer.subscriber.Subscribe(context.Background(), topc)
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
