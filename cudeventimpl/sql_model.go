package cudeventimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/cudevent"
	"github.com/suifengpiao14/sqlexec"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
	"github.com/tidwall/gjson"
)

var SoftDeleteColumn = "deleted_at" // 当update语句出现该列时，当成删除操作

//var tablePrimaryKeyMap sync.Map

// func getTablePrimaryKeyMapKey(database string, table string) (key string) {
// 	return fmt.Sprintf("%s_%s", database, table)
// }

// func RegisterTablePrimaryKey(database string, table string, primaryKey BaseField) {
// 	key := getTablePrimaryKeyMapKey(database, table)
// 	tablePrimaryKeyMap.Store(key, &primaryKey)
// }

var (
	ERROR_NOT_FOUND_PRIMARY_KEY_BY_TABLE_NAME = errors.New("not found primary key by table name")
	ERROR_INVALID_TYPE                        = errors.New("invalid type")
)

func GetPrimaryKey(database string, tableName string) (primaries sqlexecparser.Columns, err error) {
	table, err := sqlexecparser.GetTable(sqlexecparser.DBName(database), sqlexecparser.TableName(tableName))
	if err != nil {
		return nil, err
	}
	primaries, err = table.GetPrimaryKey()
	if err != nil {
		err = errors.WithMessagef(err, "database:%s", database)
		return nil, err
	}
	return primaries, nil
}

type SQLModel struct {
	PrimaryColumns sqlexecparser.Columns
	Table          string
	data           []byte
}

func (m SQLModel) GetIdentity() (identifyKv cudevent.IdentifyKv) {
	arr := make([]string, 0)
	for _, c := range m.PrimaryColumns {
		arr = append(arr, fmt.Sprintf("%s=%s", c.ColumnName, c.ColumnName))
	}
	gpath := fmt.Sprintf(`{%s}`, strings.Join(arr, ","))
	data := gjson.GetBytes(m.data, gpath).String()
	if data == "" {
		return identifyKv
	}
	identifyKv = make(cudevent.IdentifyKv)
	err := json.Unmarshal([]byte(data), &identifyKv)
	if err != nil {
		panic(err)
	}
	return identifyKv
}

func (m SQLModel) GetDomain() (domain string) {
	return m.Table
}
func (m SQLModel) GetJsonData() (jsonData []byte, err error) {
	return m.data, nil
}

type SQLModels []SQLModel

func (ms SQLModels) ToCUDEmiter() (cudEmiter cudevent.CUDEmiter) {
	cudEmiter = make(cudevent.CUDEmiter, 0)
	for _, m := range ms {
		cudEmiter = append(cudEmiter, m)
	}
	return cudEmiter
}

func (ms SQLModels) GetIdentities() (identities []cudevent.IdentifyKv) {
	identities = make([]cudevent.IdentifyKv, 0)
	for _, m := range ms {
		identities = append(identities, m.GetIdentity())
	}
	return identities
}

func (ms SQLModels) GetPrimaryKey() (primaryKey sqlexecparser.Columns) {
	for _, m := range ms {
		return m.PrimaryColumns
	}
	return nil
}

type SQLRawEvent struct {
	Stmt         sqlparser.Statement `json:"-"`
	DB           *sql.DB             `json:"-"`
	Database     string              `json:"database"` //这个用来获取ddl相关数据,以及依赖ddl 生成的table内容
	SQL          string              `json:"sql"`
	LastInsertId string              `json:"lastInsertId"`
	RowsAffected int64               `json:"affectedRows"`
	BeforeData   string              `json:"-"` // update 更新前的数据
}

func PublishSQLRawEvent(sqlRawEvent *SQLRawEvent) (err error) {
	if sqlRawEvent.SQL == "" {
		err = errors.New("SQL required")
		return err
	}
	if sqlRawEvent.DB == nil {
		err = errors.New("DB required")
		return err
	}

	if sqlRawEvent.Database == "" {
		err = errors.New("Database required")
		return err
	}
	stmt, err := sqlparser.Parse(sqlRawEvent.SQL)
	if err != nil {
		return err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return emitInsertEvent(sqlRawEvent, stmt)
	case *sqlparser.Update:
		deleteColumn := &sqlparser.ColName{Name: sqlparser.NewColIdent(SoftDeleteColumn)}
		isDelete := false
		for _, expr := range stmt.Exprs {
			isDelete = expr.Name.Equal(deleteColumn)
			if isDelete {
				break
			}
		}

		if isDelete { //软删除
			return emitSoftDeleteEvent(sqlRawEvent, stmt)
		}
		return emitUpdatedEvent(sqlRawEvent, stmt)
	case *sqlparser.Delete:
		return emitDeleteEvent(sqlRawEvent, stmt)
	}
	// 默认不发布事件
	return nil

}

func PublishSQLRawEventAsync(sqlRawEvent *SQLRawEvent) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				_ = rec
				panic(rec)
			}
		}()
		err := PublishSQLRawEvent(sqlRawEvent)
		panic(err)

	}()

}

func emitInsertEvent(sqlRawEvent *SQLRawEvent, stmt *sqlparser.Insert) (err error) {
	table := sqlparser.String(stmt.Table)
	primaryKeyColumns, err := GetPrimaryKey(sqlRawEvent.Database, table)
	if err != nil {
		return err
	}
	var ids []string
	err = json.Unmarshal([]byte(sqlRawEvent.LastInsertId), &ids)
	if err != nil {
		return err
	}
	colVals := make(sqlexecparser.ColumnValues, 0)
	for _, col := range primaryKeyColumns {
		cv := sqlexecparser.ColumnValue{
			Column:   col.ColumnName,
			Value:    ids,
			Operator: sqlparser.InStr,
		}
		colVals.AddIgnore(cv)
	}
	selectSQL := getByIdentifySQL(table, colVals)
	ctx := context.Background()
	data, err := sqlexec.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(sqlRawEvent.Database, table, []byte(data))
	if err != nil {
		return err
	}
	err = cudevent.EmitCreatedEvent(afterModels.ToCUDEmiter()...)
	if err != nil {
		return err
	}

	return nil
}

func emitUpdatedEvent(sqlRawEvent *SQLRawEvent, stmt *sqlparser.Update) (err error) {
	if sqlRawEvent.BeforeData == "" { // 如果更新前记录为空，则说明依据更新的条件，找不到记录
		return
	}
	table := sqlparser.String(stmt.TableExprs)
	beforModels, err := byte2SQLModels(sqlRawEvent.Database, table, []byte(sqlRawEvent.BeforeData))
	if err != nil {
		return err
	}

	primaryColumns, err := GetPrimaryKey(sqlRawEvent.Database, table)
	if err != nil {
		return err
	}
	colNames := primaryColumns.GetNames()
	columnvalues := sqlexecparser.ParseWhere(stmt.Where)
	primaryColumnvalues := columnvalues.FilterByColName(colNames...)
	selectSQL := getByIdentifySQL(table, primaryColumnvalues)
	ctx := context.Background()
	data, err := sqlexec.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(sqlRawEvent.Database, table, []byte(data))
	if err != nil {
		return err
	}
	err = cudevent.EmitUpdatedEvent(beforModels.ToCUDEmiter(), afterModels.ToCUDEmiter())
	if err != nil {
		return err
	}
	return nil
}

func emitDeleteEvent(sqlRawEvent *SQLRawEvent, stmt *sqlparser.Delete) (err error) {
	table := sqlparser.String(stmt.TableExprs[0])
	primaryColumns, err := GetPrimaryKey(sqlRawEvent.Database, table)
	if err != nil {
		return err
	}

	colNames := primaryColumns.GetNames()
	columnvalues := sqlexecparser.ParseWhere(stmt.Where)
	primaryColumnvalues := columnvalues.FilterByColName(colNames...)
	selectSQL := getByIdentifySQL(table, primaryColumnvalues)
	ctx := context.Background()
	data, err := sqlexec.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(sqlRawEvent.Database, table, []byte(data))
	if err != nil {
		return err
	}
	err = cudevent.EmitDeletedEvent(afterModels.ToCUDEmiter())
	if err != nil {
		return err
	}
	return nil
}

func emitSoftDeleteEvent(sqlRawEvent *SQLRawEvent, stmt *sqlparser.Update) (err error) {
	table := sqlparser.String(stmt.TableExprs[0])
	beforModels, err := byte2SQLModels(sqlRawEvent.Database, table, []byte(sqlRawEvent.BeforeData))
	if err != nil {
		return err
	}
	err = cudevent.EmitDeletedEvent(beforModels.ToCUDEmiter())
	if err != nil {
		return err
	}
	return nil
}

func byte2SQLModels(database string, table string, b []byte) (sqlModels SQLModels, err error) {
	primaryColumns, err := GetPrimaryKey(database, table)
	if err != nil {
		return nil, err
	}
	if !gjson.ValidBytes(b) {
		return nil, errors.New("invalid json")
	}
	result := gjson.ParseBytes(b)
	if err != nil {
		return nil, err
	}
	sqlModels = make(SQLModels, 0)
	for _, oneResult := range result.Array() {
		sqlModel := SQLModel{
			PrimaryColumns: primaryColumns,
			Table:          table,
			data:           []byte(oneResult.String()),
		}
		sqlModels = append(sqlModels, sqlModel)
	}

	return sqlModels, nil
}

const (
	PrimaryKey_Type_Int = "int"
)

//getByIdentifySQL 通过主键或者唯一索引键获取数据
func getByIdentifySQL(table string, primaryColumnvalues sqlexecparser.ColumnValues) (sql string) {
	sql = fmt.Sprintf("select * from %s %s;", table, sqlparser.String(primaryColumnvalues.WhereAndExpr()))
	return sql
}
