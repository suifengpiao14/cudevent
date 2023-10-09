package process

import (
	"context"

	"github.com/spf13/cast"
	"github.com/suifengpiao14/syncdata"
	"github.com/suifengpiao14/syncdata/emiter"
)

type CUDModel struct {
	emiter.CUDEmiterInterface
	CUDModelInterface
}

type CUDModelInterface interface {
	GetByIdentity(id string) (model emiter.CUDEmiterInterface, err error)
}

type EXECSQL interface {
	Exec(ctx context.Context, result interface{}) (err error)
}

func (cudModel CUDModel) GetCUDEventEmiter() (cudEmiter *emiter.CUDEventEmiter) {
	cudEmiter = emiter.NewCUDEventEmiter(syncdata.MemoryPublisherGetter)
	return cudEmiter
}

func (cudModel CUDModel) AddModel(entity EXECSQL) (err error) {
	var idAny interface{}
	err = entity.Exec(context.Background(), &idAny)
	if err != nil {
		return err
	}
	id := cast.ToString(idAny)
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	err = cudModel.GetCUDEventEmiter().EmitCreatedEvent(model)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel CUDModel) UpdateModel(id string, entity EXECSQL) (err error) {
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	err = entity.Exec(context.Background(), nil)
	if err != nil {
		return err
	}
	newmodel, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}

	err = cudModel.GetCUDEventEmiter().EmitUpdatedEvent(model, newmodel)
	if err != nil {
		return err
	}
	return nil
}

func (cudModel CUDModel) DelModel(id string, entity EXECSQL) (err error) {
	model, err := cudModel.GetByIdentity(id)
	if err != nil {
		return err
	}
	err = entity.Exec(context.Background(), nil)
	if err != nil {
		return err
	}

	err = cudModel.GetCUDEventEmiter().EmitDeletedEvent(model)
	if err != nil {
		return err
	}
	return
}
