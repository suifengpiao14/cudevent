package cudevent

type CUDModelInterface interface {
	CUDEmiterInterface
	GetByIdentities(ids ...any) (models CUDEmiterInterfaces, err error)
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
type CUDUpdateHandleFn func(identifies ...any) (err error)
type CUDAddHandleFn func() (identifies []any, err error)

func (cudModel CUDModel) AddModel(addFn CUDAddHandleFn) (err error) {
	ids, err := addFn()
	if err != nil {
		return err
	}
	models, err := cudModel.GetByIdentities(ids...)
	if err != nil {
		return err
	}
	err = EmitCreatedEvent(models...)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel CUDModel) UpdateModel(updateFn CUDUpdateHandleFn, ids ...any) (err error) {
	oldModels, err := cudModel.GetByIdentities(ids...)
	if err != nil {
		return err
	}
	err = updateFn(ids...)
	if err != nil {
		return err
	}
	newmodels, err := cudModel.GetByIdentities(ids...)
	if err != nil {
		return err
	}

	err = EmitUpdatedEvent(oldModels, newmodels)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel CUDModel) DelModel(deleteFn CUDUpdateHandleFn, ids ...any) (err error) {
	model, err := cudModel.GetByIdentities(ids...)
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
