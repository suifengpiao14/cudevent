package cudeventimpl_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
)

func Init() {
	cudeventimpl.RegisterTablePrimaryKey("user", cudeventimpl.BaseField{
		DB:         "test",
		Table:      "user",
		Name:       "id",
		Type:       "int",
		PrimaryKey: true,
	})
	cudeventimpl.RegisterTablePrimaryKey("department", cudeventimpl.BaseField{
		DB:         "test",
		Table:      "department",
		Name:       "id",
		Type:       "int",
		PrimaryKey: true,
	})
}

func TestSyncUpdateNamedSQL(t *testing.T) {
	Init()
	t.Run("normal", func(t *testing.T) {
		relation := "department.user_name=user.name,department.user_nickname=user.nick_name"
		relation2 := "department.user_id=user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		syncUpdateNamedSql, err := fieldRelations.SyncUpdateNamedSQL()
		require.NoError(t, err)
		expected := "update user,department set `department`.`user_name`=`user`.`name`,`department`.`user_nickname`=`user`.`nick_name` where  1=1 and `department`.`user_id`=`user`.`id` and `department`.`id`=:ID limit 1"
		assert.Equal(t, syncUpdateNamedSql, expected)
	})
	t.Run("no update field ", func(t *testing.T) {
		relation := ""
		relation2 := "department.user_id=user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncUpdateNamedSQL()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_NO_UPDATE_FIELD))
	})

	t.Run("no primary relation field", func(t *testing.T) {
		relation := "department.user_name=user.name,department.user_nickname=user.nick_name"
		relation2 := ""
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncUpdateNamedSQL()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_NO_PRIMARY_RELATION_FIELD))
	})

}
