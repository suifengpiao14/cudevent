package syncdata

import (
	"fmt"
)

var TOPIC_FORMAT = "%s.cud"

func makeTopic(domain string) (topic string) {
	topic = fmt.Sprintf(TOPIC_FORMAT, domain)
	return topic
}

// 增改删 操作广播领域事件
type CUDEmiterInterface interface {
	GetIdentity() string
	GetDomain() string
}

const (
	EVENT_TYPE_CREATED = "created"
	EVENT_TYPE_UPDATED = "updated"
	EVENT_TYPE_DELETED = "deleted"
)

//EmitCreatedEvent 创建完成后,发起创建完成领域事件
func EmitCreatedEvent(after CUDEmiterInterface) (err error) {
	topic := makeTopic(after.GetDomain())
	return emitEvent(topic, EVENT_TYPE_CREATED, after.GetIdentity(), nil, after)
}

//EmitUpdatedEvent 更新完成后,发起更新完成领域事件
func EmitUpdatedEvent(before CUDEmiterInterface, after CUDEmiterInterface) (err error) {
	topic := makeTopic(after.GetDomain())
	return emitEvent(topic, EVENT_TYPE_UPDATED, after.GetIdentity(), before, after)
}

//EmitUpdatedEvent 删除完成后,发起删除完成领域事件
func EmitDeletedEvent(before CUDEmiterInterface) (err error) {
	topic := makeTopic(before.GetDomain())
	return emitEvent(topic, EVENT_TYPE_DELETED, before.GetIdentity(), before, nil)
}

func emitEvent(topic string, eventType string, id string, before CUDEmiterInterface, after CUDEmiterInterface) (err error) {
	changedPayload, err := newChangedPayload(eventType, id, before, after)
	if err != nil {
		return err
	}
	err = publish(topic, changedPayload)
	if err != nil {
		return err
	}
	return nil

}
