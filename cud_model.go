package cudevent

import "context"

type CUDModelInterface interface {
	CUDEmiterInterface
	GetByIdentities(ctx context.Context, ids ...string) (models CUDEmiterInterfaces, err error)
}

type CUDModel struct {
	CUDModelInterface
}

func NewCUDModel(cudModelImpl CUDModelInterface) (cudModel *CUDModel) {
	return &CUDModel{
		CUDModelInterface: cudModelImpl,
	}
}

// CUDUpdateHandleFn 增改删句柄函数
type CUDUpdateHandleFn func(identifies ...string) (err error)
type CUDAddHandleFn func(uniqIds ...string) (identifies []string, err error)

func (cudModel CUDModel) AddModel(ctx context.Context, addFn CUDAddHandleFn) (err error) {
	ids, err := addFn()
	if err != nil {
		return err
	}
	models, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}
	err = EmitCreatedEvent(models...)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel CUDModel) UpdateModel(ctx context.Context, updateFn CUDUpdateHandleFn, ids ...string) (err error) {
	oldModels, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}
	err = updateFn(ids...)
	if err != nil {
		return err
	}
	newmodels, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}

	err = EmitUpdatedEvent(oldModels, newmodels)
	if err != nil {
		return err
	}
	return nil
}

// SetModel 筛选出新增发布创建事件，更新发布更新事件
func (cudModel CUDModel) SetModel(ctx context.Context, addFn CUDAddHandleFn, updateFn CUDUpdateHandleFn, ids ...string) (err error) {
	oldModels, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}
	updateIds := make([]string, 0)
	for _, model := range oldModels {
		updateIds = append(updateIds, model.GetIdentity())
	}
	addIds := make([]string, 0)
	for _, id := range ids {
		isUpdateId := false
		for _, updateId := range updateIds {
			isUpdateId = updateId == id
			if isUpdateId {
				break
			}
		}
		if !isUpdateId {
			addIds = append(addIds, id)
		}
	}
	insertIds := make([]string, 0)
	if len(addIds) > 0 {
		insertIds, err = addFn(addIds...)
		if err != nil {
			return err
		}
	}
	if len(updateIds) > 0 {
		err = updateFn(updateIds...)
		if err != nil {
			return err
		}
	}

	newmodels, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}
	insertModels := make(CUDEmiterInterfaces, 0)
	updateModels := make(CUDEmiterInterfaces, 0)

	for _, model := range newmodels {
		isInsertModel := false
		id := model.GetIdentity()
		for _, insertId := range insertIds {
			isInsertModel = insertId == id
			if isInsertModel {
				break
			}
		}

		if isInsertModel {
			insertModels = append(insertModels, model)
		} else {
			updateModels = append(updateModels, model)
		}
	}

	if len(insertModels) > 0 {
		err = EmitCreatedEvent(insertModels...)
		if err != nil {
			return err
		}
	}

	if len(oldModels) > 0 && len(updateModels) > 0 {
		err = EmitUpdatedEvent(oldModels, updateModels)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cudModel CUDModel) DelModel(ctx context.Context, deleteFn CUDUpdateHandleFn, ids ...string) (err error) {
	model, err := cudModel.GetByIdentities(ctx, ids...)
	if err != nil {
		return err
	}
	err = deleteFn(ids...)
	if err != nil {
		return err
	}

	err = EmitDeletedEvent(model)
	if err != nil {
		return err
	}
	return
}
