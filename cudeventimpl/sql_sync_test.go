package cudeventimpl_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func Init() (err error) {
	ddl := "./ddl.sql"
	b, err := os.ReadFile(ddl)
	if err != nil {
		return err
	}
	err = sqlexecparser.RegisterTableByDDL(string(b))
	if err != nil {
		return err
	}
	return nil
}

// todo 增加database维度后未通过测试,临时提交
func TestSyncUpdateNamedSQL(t *testing.T) {

	t.Run("normal", func(t *testing.T) {
		err := Init()
		require.NoError(t, err)
		relation := "oa.department.user_name=oa.user.name,oa.department.user_nickname=oa.user.nickname"
		relation2 := "oa.department.user_id=oa.user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		syncUpdateNamedSql, err := fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		require.NoError(t, err)
		expected := "update oa.department,oa.user set oa.department.user_name=oa.user.name,oa.department.user_nickname=oa.user.nickname where 1=1 and oa.department.user_id=oa.user.id and oa.department.id = :id;"
		assert.Equal(t, expected, syncUpdateNamedSql)
	})
	t.Run("no update field ", func(t *testing.T) {
		err := Init()
		require.NoError(t, err)
		relation := ""
		relation2 := "oa.department.user_id=oa.user.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_NO_UPDATE_FIELD))
	})

	t.Run("no primary relation field", func(t *testing.T) {
		err := Init()
		require.NoError(t, err)
		relation := "oa.department.user_name=oa.user.name,oa.department.user_nickname=oa.user.nickname"
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
	export.export_task.title=export.export_template.title,
	export.export_task.timeout=export.export_template.max_exec_time,
	export.export_task.template_id=export.export_template.id,
	`
	relation, err := cudeventimpl.ParseFieldRelation(relationStr)
	require.NoError(t, err)
	fmt.Println(relation)
}

func TestSyncRedundantFieldBySrcPrimaryKey(t *testing.T) {
	Init()
	relationStr := `
	export.export_task.timeout=export.export_template.max_exec_time,
	export.export_task.template_id=export.export_template.id,
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
