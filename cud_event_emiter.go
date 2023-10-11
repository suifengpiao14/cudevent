package cudevent

import (
	"fmt"

	"github.com/pkg/errors"
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

type CUDEmiterInterfaces []CUDEmiterInterface

func (emiters CUDEmiterInterfaces) GetByIdentity(identity string) (emiter CUDEmiterInterface, ok bool) {
	for _, e := range emiters {
		if identity == emiter.GetIdentity() {
			return e, true
		}
	}
	return nil, false
}

const (
	EVENT_TYPE_CREATED = "created"
	EVENT_TYPE_UPDATED = "updated"
	EVENT_TYPE_DELETED = "deleted"
)

var (
	ERROR_MODELS_NUMBER                             = errors.New("the number of models must be greater than 0")
	ERROR_UPDATE_MODEL_COUNT_BEFORE_AFTER           = errors.New("the number of models before and after updating must be equal")
	ERROR_UPDATE_MODEL_OBJECT_NOT_SAME_BEFORE_AFTER = errors.New("object inconsistency before and after update")
	ERROR_UPDATE_MODEL_DOMAIN_NOT_SAME_BEFORE_AFTER = errors.New("domain inconsistency before and after update")
	ERROR_UPDATE_MODEL_DOMAIN_INCONSISTENT          = errors.New("Inconsistent model domains")
)

//EmitCreatedEvent 创建完成后,发起创建完成领域事件
func EmitCreatedEvent(afterModels ...CUDEmiterInterface) (err error) {
	if len(afterModels) == 0 {
		return ERROR_MODELS_NUMBER
	}
	afterModel := afterModels[0]
	return emitEvent(afterModel.GetDomain(), EVENT_TYPE_CREATED, nil, afterModels)
}

//EmitUpdatedEvent 更新完成后,发起更新完成领域事件
func EmitUpdatedEvent(beforeModels CUDEmiterInterfaces, afterModels CUDEmiterInterfaces) (err error) {
	if len(afterModels) == 0 {
		return ERROR_MODELS_NUMBER
	}
	/** -----------检测参数开始------------------ **/
	afterModel := afterModels[0]
	beforeCount := len(beforeModels)
	afterCount := len(afterModels)
	if beforeCount != afterCount {
		err = errors.WithMessagef(ERROR_UPDATE_MODEL_COUNT_BEFORE_AFTER, "befor count:%d,after count:%d", beforeCount, afterCount)
		return
	}
	afterModelMap := make(map[string]CUDEmiterInterface)
	for _, afterModel := range afterModels {
		afterModelMap[afterModel.GetIdentity()] = afterModel
	}
	domainCheckMap := make(map[string]CUDEmiterInterface)
	for _, beforeModel := range beforeModels {
		afterModel, ok := afterModelMap[beforeModel.GetIdentity()]
		if !ok {
			err = errors.WithMessagef(ERROR_UPDATE_MODEL_OBJECT_NOT_SAME_BEFORE_AFTER, "before model:%s not in after models", beforeModel.GetIdentity())
			return err
		}
		if afterModel.GetDomain() != beforeModel.GetDomain() {
			err = errors.WithMessagef(ERROR_UPDATE_MODEL_DOMAIN_NOT_SAME_BEFORE_AFTER, "model:%s befor domain is:%s,after domain is:%s", beforeModel.GetIdentity(), beforeModel.GetDomain(), afterModel.GetDomain())
			return err
		}
		if len(domainCheckMap) == 0 {
			domainCheckMap[beforeModel.GetDomain()] = beforeModel
		} else {
			if _, ok := domainCheckMap[beforeModel.GetDomain()]; !ok {
				var existsModel CUDEmiterInterface
				for _, existsModel = range domainCheckMap {
					break
				}
				err = errors.WithMessagef(ERROR_UPDATE_MODEL_DOMAIN_INCONSISTENT, "model:%s's domain is:%s ,bud model:%s's domain is:%s", existsModel.GetIdentity(), existsModel.GetDomain(), beforeModel.GetIdentity(), afterModel.GetDomain())
				return err
			}
		}
	}
	/** -----------检测参数结束------------------ **/

	return emitEvent(afterModel.GetDomain(), EVENT_TYPE_UPDATED, beforeModels, afterModels)
}

//EmitUpdatedEvent 删除完成后,发起删除完成领域事件
func EmitDeletedEvent(beforeModels CUDEmiterInterfaces) (err error) {
	if len(beforeModels) == 0 {
		return ERROR_MODELS_NUMBER
	}
	beforeModel := beforeModels[0]
	return emitEvent(beforeModel.GetDomain(), EVENT_TYPE_DELETED, beforeModels, nil)
}

func emitEvent(domain string, eventType string, before CUDEmiterInterfaces, afterModels CUDEmiterInterfaces) (err error) {
	changedPayload, err := newChangedPayload(domain, eventType, before, afterModels)
	if err != nil {
		return err
	}
	err = publish(domain, changedPayload)
	if err != nil {
		return err
	}
	return nil
}
