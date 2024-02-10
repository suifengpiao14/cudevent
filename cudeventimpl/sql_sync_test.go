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
	database := "export"
	sqlddlFile := "./test/exportservice.sql"
	ddlByte, err := os.ReadFile(sqlddlFile)
	if err != nil {
		return err
	}
	err = sqlexecparser.RegisterTableByDDL(database, string(ddlByte))
	if err != nil {
		return err
	}
	return nil
}

func TestSyncUpdateNamedSQL(t *testing.T) {
	Init()
	t.Run("normal", func(t *testing.T) {
		relation := "export.export_task.template_name=export.export_template.template_name,export.export_task.title=export.export_task.title"
		relation2 := "export.export_task.template_id=export.export_template.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		syncUpdateNamedSql, err := fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		require.NoError(t, err)
		expected := "update export_task,export_template set `export_task`.`template_name`=`export_template`.`template_name`,`export_task`.`title`=`export_template`.`title` where  1=1 and `export_task`.`template_id`=`export_template`.`id` and `export_task`.`id`=:ID limit 1"
		assert.Equal(t, syncUpdateNamedSql, expected)
	})
	t.Run("no update field ", func(t *testing.T) {
		relation := ""
		relation2 := "export.export_task.template_id=export.export_template.id"
		fieldRelations, err := cudeventimpl.ParseFieldRelation(relation, relation2)
		require.NoError(t, err)
		_, err = fieldRelations.SyncRedundantFieldByDstPrimaryKey()
		assert.Equal(t, true, errors.Is(err, cudeventimpl.ERROR_NO_UPDATE_FIELD))
	})

	t.Run("no primary relation field", func(t *testing.T) {
		relation := "export.export_task.template_name=export.export_template.template_name,export.export_task.title=export.export_task.title"
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
