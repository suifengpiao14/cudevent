package cudevent

import (
	"encoding/json"
	"sort"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var TOPIC_FORMAT = "cud.%s"

type IdentifyKv map[string]any

func (ikv IdentifyKv) String() string {
	b, err := json.Marshal(ikv)
	if err != nil {
		panic(err)
	}
	return string(b)
}

//GetFirstValue 自动递增ID时有用
func (ikv IdentifyKv) GetFirstValue() (s string) {
	ks := make([]string, 0)
	for k := range ikv {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		v := ikv[k]
		s = cast.ToString(v)
		return s
	}
	return
}

// 增改删 操作广播领域事件
type CUDEmiterI interface {
	DiffI
	GetIdentity() IdentifyKv
	GetDomain() string
}

type CUDEmiter []CUDEmiterI

func (emiters CUDEmiter) GetByIdentity(identityNames ...string) (emiter CUDEmiterI, ok bool) {
	for _, e := range emiters {
		identity := e.GetIdentity()
		if identity == nil {
			continue
		}
		if len(identity) != len(identityNames) {
			continue
		}
		eq := true
		for _, name := range identityNames {
			if _, ok := identity[name]; !ok {
				eq = false
				break
			}
		}
		if eq {
			emiter = e
			return emiter, true
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

// EmitCreatedEvent 创建完成后,发起创建完成领域事件
func EmitCreatedEvent(afterModels ...CUDEmiterI) (err error) {
	if len(afterModels) == 0 {
		return ERROR_MODELS_NUMBER
	}
	afterModel := afterModels[0]
	return emitEvent(afterModel.GetDomain(), EVENT_TYPE_CREATED, nil, afterModels)
}

// EmitUpdatedEvent 更新完成后,发起更新完成领域事件
func EmitUpdatedEvent(beforeModels CUDEmiter, afterModels CUDEmiter) (err error) {
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
	afterModelMap := make(map[string]CUDEmiterI)
	for _, afterModel := range afterModels {
		afterModelMap[afterModel.GetIdentity().String()] = afterModel
	}
	tableCheckMap := make(map[string]CUDEmiterI)
	for _, beforeModel := range beforeModels {
		afterModel, ok := afterModelMap[beforeModel.GetIdentity().String()]
		if !ok {
			err = errors.WithMessagef(ERROR_UPDATE_MODEL_OBJECT_NOT_SAME_BEFORE_AFTER, "before model:%s not in after models", beforeModel.GetIdentity())
			return err
		}
		if afterModel.GetDomain() != beforeModel.GetDomain() {
			err = errors.WithMessagef(ERROR_UPDATE_MODEL_DOMAIN_NOT_SAME_BEFORE_AFTER, "model:%s befor table is:%s,after table is:%s", beforeModel.GetIdentity(), beforeModel.GetDomain(), afterModel.GetDomain())
			return err
		}
		if len(tableCheckMap) == 0 {
			tableCheckMap[beforeModel.GetDomain()] = beforeModel
		} else {
			if _, ok := tableCheckMap[beforeModel.GetDomain()]; !ok {
				var existsModel CUDEmiterI
				for _, existsModel = range tableCheckMap {
					break
				}
				err = errors.WithMessagef(ERROR_UPDATE_MODEL_DOMAIN_INCONSISTENT, "model:%s's table is:%s ,bud model:%s's table is:%s", existsModel.GetIdentity(), existsModel.GetDomain(), beforeModel.GetIdentity(), afterModel.GetDomain())
				return err
			}
		}
	}
	/** -----------检测参数结束------------------ **/

	return emitEvent(afterModel.GetDomain(), EVENT_TYPE_UPDATED, beforeModels, afterModels)
}

// EmitUpdatedEvent 删除完成后,发起删除完成领域事件
func EmitDeletedEvent(beforeModels CUDEmiter) (err error) {
	if len(beforeModels) == 0 {
		return ERROR_MODELS_NUMBER
	}
	beforeModel := beforeModels[0]
	return emitEvent(beforeModel.GetDomain(), EVENT_TYPE_DELETED, beforeModels, nil)
}

func emitEvent(table string, eventType string, before CUDEmiter, afterModels CUDEmiter) (err error) {
	payloads, err := newChangedPayload(before, afterModels)
	if err != nil {
		return err
	}
	changedPayload := &_ChangedMessage{
		Table:     table,
		EventType: eventType,
		Payloads:  payloads,
	}
	// 过滤掉关注的数据没有发生实际变化的场景
	isPublish := false
	for _, payload := range changedPayload.Payloads {
		if payload.Before != payload.After {
			isPublish = true
			break
		}
	}
	if isPublish {
		err = publish(changedPayload)
		if err != nil {
			return err
		}
	}

	return nil
}
