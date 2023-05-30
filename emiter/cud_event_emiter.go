package emiter

import (
	"fmt"

	"github.com/suifengpiao14/syncdata"
)

// 增改删 操作广播领域事件

type CUDEntity interface {
	GetIdentity() string
	GetDomain() string
}

type CUDEventEmiter struct {
	getPublisher syncdata.GetPublisherFn
}

func NewCUDEventEmiter(getPublisher syncdata.GetPublisherFn) (emiter *CUDEventEmiter) {
	emiter = &CUDEventEmiter{
		getPublisher: getPublisher,
	}
	return emiter
}

func (cud CUDEventEmiter) GetTopic(domain string) (topic string) {
	return makeTopic(domain)
}

//EmitCreatedEvent 创建完成后,发起创建完成领域事件
func (cud CUDEventEmiter) EmitCreatedEvent(after CUDEntity) (err error) {
	topic := cud.GetTopic(after.GetDomain())
	return cud.emitEvent(topic, syncdata.EVENT_TYPE_DELETED, after.GetIdentity(), nil, after)
}

//EmitUpdatedEvent 更新完成后,发起更新完成领域事件
func (cud CUDEventEmiter) EmitUpdatedEvent(before CUDEntity, after CUDEntity) (err error) {
	topic := cud.GetTopic(after.GetDomain())
	return cud.emitEvent(topic, syncdata.EVENT_TYPE_DELETED, after.GetIdentity(), before, after)
}

//EmitUpdatedEvent 删除完成后,发起删除完成领域事件
func (cud CUDEventEmiter) EmitDeletedEvent(before CUDEntity) (err error) {
	topic := cud.GetTopic(before.GetDomain())
	return cud.emitEvent(topic, syncdata.EVENT_TYPE_DELETED, before.GetIdentity(), before, nil)
}

func (cud CUDEventEmiter) emitEvent(topic string, eventType string, id string, before CUDEntity, after CUDEntity) (err error) {
	changedPayload, err := syncdata.NewChangedPayload(eventType, id, before, after)
	if err != nil {
		return err
	}
	publisher, err := cud.getPublisher()
	if err != nil {
		return err
	}
	err = syncdata.Publish(publisher, topic, changedPayload)
	if err != nil {
		return err
	}
	return nil

}

func makeTopic(domain string) (topic string) {
	topic = fmt.Sprintf("%s.cud", domain)
	return topic
}
