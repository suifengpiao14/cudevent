package cudevent

type CUDModelInterface interface {
	CUDEmiterInterface
	GetByIdentity(id string) (model CUDEmiterInterface, err error)
}

type _CUDModel struct {
	CUDModelInterface
}

func NewCUDModel(cudModelImpl CUDModelInterface) (cudModel _CUDModel) {
	return _CUDModel{
		CUDModelInterface: cudModelImpl,
	}
}

// CUDHandleFn 增改删句柄函数
type CUDHandleFn func() (id string, err error)

func (cudModel _CUDModel) AddModel(addFn CUDHandleFn) (err error) {
	id, err := addFn()
	if err != nil {
		return err
	}
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	err = EmitCreatedEvent(model)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel _CUDModel) UpdateModel(id string, updateFn CUDHandleFn) (err error) {
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	_, err = updateFn()
	if err != nil {
		return err
	}
	newmodel, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}

	err = EmitUpdatedEvent(model, newmodel)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel _CUDModel) DelModel(id string, deleteFn CUDHandleFn) (err error) {
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	_, err = deleteFn()
	if err != nil {
		return err
	}

	err = EmitDeletedEvent(model)
	if err != nil {
		return err
	}
	return
}
