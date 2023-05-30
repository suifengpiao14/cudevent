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

type DealEventFn func(msg *message.Message) (err error)

//DealEventFnEmpty 不做任何处理，直接返回nil (可以用于跑流程)
func DealEventFnEmpty(msg *message.Message) (err error) {
	return nil
}

type GetSubscriberFn func() (subscriber message.Subscriber, err error)

func Subscriber(topc string, getSubscriberFn GetSubscriberFn, fn DealEventFn) (err error) {
	subscriber, err := getSubscriberFn()
	if err != nil {
		return err
	}
	messageChan, err := subscriber.Subscribe(context.Background(), topc)
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
