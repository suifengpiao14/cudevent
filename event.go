package cudevent

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/pkg/errors"
)

// 变化前后的负载
type _ChangedMessage struct {
	Domain    string     `json:"domain"`
	EventType string     `json:"eventType"`
	Payload   []_Payload `json:"payload"`
}

type _Payload struct {
	ID     any    `json:"id"`
	Before string `json:"befor"`
	After  string `json:"after"`
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

func newChangedPayload(domain string, eventType string, beforeEmiters CUDEmiter, afterEmiters CUDEmiter) (changedMessage *_ChangedMessage, err error) {
	changedMessage = &_ChangedMessage{
		Domain:    domain,
		EventType: eventType,
		Payload:   make([]_Payload, 0),
	}
	if len(beforeEmiters) == 0 && len(afterEmiters) > 0 {
		for _, after := range afterEmiters {
			payload := diffEmiter2Payload(after.GetIdentity(), nil, after.GetJsonData())
			changedMessage.Payload = append(changedMessage.Payload, payload)
		}
		return
	}
	if len(afterEmiters) == 0 && len(beforeEmiters) > 0 {
		for _, before := range beforeEmiters {
			payload := diffEmiter2Payload(before.GetIdentity(), nil, before.GetJsonData())
			changedMessage.Payload = append(changedMessage.Payload, payload)
		}
		return
	}
	for _, befor := range beforeEmiters {
		after, _ := afterEmiters.GetByIdentity(befor.GetIdentity())
		var oldPatch, newPatch []byte
		if befor != nil && after != nil { // 变化前后都有值时，对比保留发生变化的属性
			oldPatch, newPatch, err = diff(befor, after)
			if err != nil {
				return nil, err
			}
		}
		payload := diffEmiter2Payload(befor.GetIdentity(), oldPatch, newPatch)
		changedMessage.Payload = append(changedMessage.Payload, payload)
	}
	return changedMessage, nil
}

func diffEmiter2Payload(id string, oldPatch []byte, newPatch []byte) (payload _Payload) {
	payload = _Payload{
		ID:     id,
		Before: string(oldPatch),
		After:  string(newPatch),
	}
	return payload

}

var ERROR_NO_DIFF_PATCH = errors.Errorf("no diff patch")

type DiffI interface {
	GetJsonData() (jsonData []byte)
}

// diff 比较2个结构体，提取前后有变化的内容(属性)
func diff(befor DiffI, after DiffI) (old []byte, new []byte, err error) {
	bforeByte := befor.GetJsonData()
	afterByte := after.GetJsonData()
	newPatch, err := jsonpatch.CreateMergePatch(bforeByte, afterByte)
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
	return oldPatch, newPatch, nil
}
