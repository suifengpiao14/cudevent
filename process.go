package syncdata

import (
	"encoding/json"
	"reflect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var pubSub = gochannel.NewGoChannel(
	gochannel.Config{},
	watermill.NewStdLogger(false, false),
)

const (
	TOPIC = "syncdata"
)

// 默认容器
var defaultContainer = NewContainer(TOPIC, pubSub, pubSub)

func GetContainer() *_Container {
	return defaultContainer
}
func SetContainer(c *_Container) {
	defaultContainer = c
}

type _Container struct {
	publisher  message.Publisher
	subscriber message.Subscriber
}

func NewContainer(topic string, publisher message.Publisher, subscriber message.Subscriber) (container *_Container) {

	container = &_Container{
		publisher:  publisher,
		subscriber: subscriber,
	}
	return
}

// 变化前后的负载
type ChangedPayload struct {
	EventType string      `json:"eventType"`
	ID        interface{} `json:"id"`
	Before    interface{} `json:"befor"`
	After     interface{} `json:"after"`
}

func (changedPayload ChangedPayload) ToMessage() (msg *message.Message) {
	b, _ := json.Marshal(changedPayload)
	msg = message.NewMessage(watermill.NewULID(), b)
	return msg
}

func (changedPayload *ChangedPayload) UmarshMessage(msg *message.Message) (err error) {
	err = json.Unmarshal(msg.Payload, changedPayload)
	return err
}

func NewChangedPayload(id interface{}, befor interface{}, after interface{}) (changedPayload *ChangedPayload) {
	changedPayload = &ChangedPayload{
		ID:     id,
		Before: befor,
		After:  after,
	}
	return changedPayload
}

const (
	EVENT_TYPE_CREATED = "created"
	EVENT_TYPE_UPDATED = "updated"
	EVENT_TYPE_DELETED = "deleted"
)

type Event struct {
	Topic    string `json:"topic"`
	EventID  string `json:"eventId"`
	Type     string `json:"type"`
	SourceID Fields `json:"primary"`
	OldAttr  Fields `json:"old"`
	NewAttr  Fields `json:"new"`
}

type RunContext struct {
	Name         string       `json:"name"`
	Config       string       `json:"config"`
	Script       string       `json:"script"`
	OutputSchema string       `json:"outputSchema"`
	Input        Fields       `json:"input"`
	Dependencies []RunContext `json:"Dependencies"`
}

type Field struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

func (f Field) GetValue(dst interface{}) {
	var value interface{}
	switch f.Type {
	case "int":
		value = cast.ToInt(f.Value)
	case "float":
		value = cast.ToFloat64(f.Value)
	case "bool":
		value = cast.ToBool(f.Value)
	default:
		value = f.Value
	}
	rv := reflect.Indirect(reflect.ValueOf(dst))
	if !rv.CanSet() {
		err := errors.Errorf("dst want ptr , got:%T", dst)
		panic(err)
	}
	rv.Set(reflect.Indirect(reflect.ValueOf(value)))
}

type Fields []Field

func (fields Fields) GetValue(name string, value interface{}) (ok bool) {
	for _, field := range fields {
		if field.Name == name {
			field.GetValue(value)
			return true
		}
	}
	return false
}
func (fields Fields) Map() (m map[string]interface{}) {
	m = make(map[string]interface{})
	for _, field := range fields {
		var value interface{}
		field.GetValue(&value)
		m[field.Name] = value
	}
	return m
}
