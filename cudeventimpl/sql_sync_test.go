package cudeventimpl_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
)

func Init() {
	database := "test"
	cudeventimpl.RegisterTablePrimaryKey(database, "user", cudeventimpl.BaseField{
		Database:   database,
		Table:      "user",
		Column:     "id",
		Type:       "int",
		PrimaryKey: true,
	})
	cudeventimpl.RegisterTablePrimaryKey(database, "department", cudeventimpl.BaseField{
		Database:   database,
		Table:      "department",
		Column:     "id",
		Type:       "int",
		PrimaryKey: true,
	})
	cudeventimpl.RegisterTablePrimaryKey(database, "export_task", cudeventimpl.BaseField{
		Database:   database,
		Table:      "export_task",
		Column:     "id",
		Type:       "int",
		PrimaryKey: true,
	})
	cudeventimpl.RegisterTablePrimaryKey(database, "export_template", cudeventimpl.BaseField{
		Database:   database,
		Table:      "export_template",
		Column:     "id",
		Type:       "int",
		PrimaryKey: true,
	})
}

// todo 增加database维度后未通过测试,临时提交
func TestSyncUpdateNamedSQL(t *testing.T) {
	Init()
	t.Run("normal", func(t *testing.T) {
		relation := "department.user_name=user.name,department.user_nickname=user.nick_name"
		relation2 := "department.user_id=user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		syncUpdateNamedSql, err := fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		require.NoError(t, err)
		expected := "update user,department set `department`.`user_name`=`user`.`name`,`department`.`user_nickname`=`user`.`nick_name` where  1=1 and `department`.`user_id`=`user`.`id` and `department`.`id`=:ID limit 1"
		assert.Equal(t, syncUpdateNamedSql, expected)
	})
	t.Run("no update field ", func(t *testing.T) {
		relation := ""
		relation2 := "department.user_id=user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_NO_UPDATE_FIELD))
	})

	t.Run("no primary relation field", func(t *testing.T) {
		relation := "department.user_name=user.name,department.user_nickname=user.nick_name"
		relation2 := ""
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_PRIMARY_RELATION_FIELD_LESS_THEN_SRC_TABLE_COUNT))
	})

}

func TestParseRelation(t *testing.T) {
	Init()
	relationStr := `
	export_task.title=export_template.title,
	export_task.timeout=export_template.max_exec_time,
	export_task.template_id=export_template.id,
	`
	relation, err := cudeventimpl.ParseFieldRelation(relationStr)
	require.NoError(t, err)
	fmt.Println(relation)
}

func TestSyncRedundantFieldBySrcPrimaryKey(t *testing.T) {
	Init()
	relationStr := `
	export_task.timeout=export_template.max_exec_time,
	export_task.template_id=export_template.id,
	`
	relation, err := cudeventimpl.ParseFieldRelation(relationStr)
	require.NoError(t, err)
	relation.SetScene(cudeventimpl.Relation_Scene_Src_Update)
	namedSqls, err := relation.SyncRedundantFieldBySrcPrimaryKey()
	require.NoError(t, err)
	for _, namedSql := range namedSqls {
		fmt.Println(namedSql)
	}
}
