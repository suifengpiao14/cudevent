package cudevent

import (
	"encoding/json"
	"reflect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
)

// 变化前后的负载
type _ChangedMessage struct {
	Domain    string     `json:"domain"`
	EventType string     `json:"eventType"`
	Payload   []_Payload `json:"payload"`
}

type _Payload struct {
	ID     any `json:"id"`
	Before any `json:"befor"`
	After  any `json:"after"`
}

func (changedPayload _ChangedMessage) ToMessage() (msg *message.Message) {
	b, _ := json.Marshal(changedPayload)
	msg = message.NewMessage(watermill.NewULID(), b)
	return msg
}

func (changedPayload *_ChangedMessage) UmarshMessage(msg *message.Message) (err error) {
	err = json.Unmarshal(msg.Payload, changedPayload)
	return err
}

func newChangedPayload(domain string, eventType string, beforeEmiters CUDEmiterInterfaces, afterEmiters CUDEmiterInterfaces) (changedMessage *_ChangedMessage, err error) {
	changedMessage = &_ChangedMessage{
		Domain:    domain,
		EventType: eventType,
		Payload:   make([]_Payload, 0),
	}
	if len(beforeEmiters) == 0 && len(afterEmiters) > 0 {
		after := afterEmiters[0]
		payload := diffEmiter2Payload(after.GetIdentity(), nil, afterEmiters)
		changedMessage.Payload = append(changedMessage.Payload, payload)
		return
	}
	if len(afterEmiters) == 0 && len(beforeEmiters) > 0 {
		before := beforeEmiters[0]
		payload := diffEmiter2Payload(before.GetIdentity(), nil, beforeEmiters)
		changedMessage.Payload = append(changedMessage.Payload, payload)
		return
	}
	for _, befor := range beforeEmiters {
		after, _ := afterEmiters.GetByIdentity(befor.GetIdentity())
		var old, new any
		if befor != nil && after != nil { // 变化前后都有值时，对比保留发生变化的属性
			old, new, err = diff(befor, after)
			if err != nil {
				return nil, err
			}
		}
		payload := diffEmiter2Payload(befor.GetIdentity(), old, new)
		changedMessage.Payload = append(changedMessage.Payload, payload)
	}
	return changedMessage, nil
}

func diffEmiter2Payload(id string, old any, new any) (payload _Payload) {
	payload = _Payload{
		ID:     id,
		Before: old,
		After:  new,
	}
	return payload

}

var ERROR_NO_DIFF_PATCH = errors.Errorf("no diff patch")

// diff 比较2个结构体，提取前后有变化的内容(属性)
func diff(befor interface{}, after interface{}) (old interface{}, new interface{}, err error) {
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
