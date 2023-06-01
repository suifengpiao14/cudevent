package syncdata

import (
	"encoding/json"
	"reflect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	jsonpatch "github.com/evanphx/json-patch"
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

//GetMemoryPublisher 内置发布者
var MemoryPublisherGetter = func() (publisher message.Publisher, err error) {
	return pubSub, nil
}
var MemorySubscriberGetter = func() (subscriber message.Subscriber, err error) {
	return pubSub, nil
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

func NewChangedPayload(eventType string, id interface{}, befor interface{}, after interface{}) (changedPayload *ChangedPayload, err error) {
	old, new := befor, after
	if old != nil && new != nil { // 变化前后都有值时，对比保留发生变化的属性
		old, new, err = Diff(befor, after)
		if err != nil {
			return nil, err
		}
	}

	changedPayload = &ChangedPayload{
		EventType: eventType,
		ID:        id,
		Before:    old,
		After:     new,
	}
	return changedPayload, nil
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

var ERROR_NO_DIFF_PATCH = errors.Errorf("no diff patch")

//Diff 比较2个结构体，提取前后有变化的内容(属性)
func Diff(befor interface{}, after interface{}) (old interface{}, new interface{}, err error) {
	rt := reflect.Indirect(reflect.ValueOf(befor)).Type()
	old, new = reflect.New(rt).Interface(), reflect.New(rt).Interface()
	bforeByte, err := json.Marshal(befor)
	if err != nil {
		return nil, nil, err
	}
	afterByte, err := json.Marshal(after)
	if err != nil {
		return nil, nil, err
	}
	newPatch, err := jsonpatch.CreateMergePatch(bforeByte, afterByte)
	if err != nil {
		return nil, nil, err
	}
	err = json.Unmarshal(newPatch, &new)
	if err != nil {
		return nil, nil, err
	}

	oldPatch, err := jsonpatch.CreateMergePatch(afterByte, bforeByte)
	if err != nil {
		return nil, nil, err
	}
	if len(oldPatch) == 0 {
		return nil, nil, ERROR_NO_DIFF_PATCH
	}

	err = json.Unmarshal(oldPatch, &old)
	if err != nil {
		return nil, nil, err
	}
	return old, new, nil
}
