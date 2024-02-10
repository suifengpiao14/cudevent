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

func GetPrimaryKey(database string, tableName string) (primaryKeys []BaseField, err error) {
	table, err := sqlexecparser.GetTable(database, tableName)
	if err != nil {
		return nil, err
	}
	constant, ok := table.Constraints.GetByType(sqlexecparser.Constraint_Type_Primary)
	if !ok {
		err = errors.Errorf("not found primary key ")
		return nil, err
	}
	primaryKeys = make([]BaseField, 0)
	for _, name := range constant.ColumnNames {
		column, ok := table.Columns.GetByName(name)
		if !ok {
			err = errors.Errorf("not found primary part key :%s in table column", name)
			return nil, err
		}
		primaryKey := BaseField{
			Database:   database,
			Table:      tableName,
			Column:     column.ColumnName,
			Type:       column.Type,
			PrimaryKey: true,
		}
		primaryKeys = append(primaryKeys, primaryKey)
	}
	if len(primaryKeys) == 0 {
		err = errors.Errorf("primary keys len zero ")
		return nil, err
	}
	return primaryKeys, nil
}

type SQLModel struct {
	PrimaryKey BaseField
	Table      string
	data       []byte
}

func (m SQLModel) GetIdentity() (id string) {
	return gjson.GetBytes(m.data, m.PrimaryKey.Column).String()
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

func (ms SQLModels) GetIdentities() (identities []string) {
	identities = make([]string, 0)
	for _, m := range ms {
		identities = append(identities, m.GetIdentity())
	}
	return identities
}

func (ms SQLModels) GetPrimaryKey() (primaryKey *BaseField) {
	for _, m := range ms {
		return &m.PrimaryKey
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

// getIdentityFromWhere 从where 条件中获取主键条件——需完善
func getIdentityFromWhere(whereExpr sqlparser.Expr, identityKey string) (expr sqlparser.Expr) {
	colIdent := sqlparser.NewColIdent(identityKey)
	identityCol := &sqlparser.ColName{Name: colIdent}
	switch expr := whereExpr.(type) {
	case *sqlparser.ComparisonExpr:
		if colExpr, ok := expr.Left.(*sqlparser.ColName); ok {
			if colExpr.Equal(identityCol) {
				return expr.Right
			}
		}
		return getIdentityFromWhere(expr.Left, identityKey)

	}
	return
}

func emitInsertEvent(sqlRawEvent *SQLRawEvent, stmt *sqlparser.Insert) (err error) {
	table := sqlparser.String(stmt.Table)
	primaryKeys, err := GetPrimaryKey(sqlRawEvent.Database, table)
	if err != nil {
		return err
	}
	var ids []string
	err = json.Unmarshal([]byte(sqlRawEvent.LastInsertId), &ids)
	if err != nil {
		return err
	}
	selectSQL := getByIDsSQL(table, primaryKeys[0], ids)
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

	ids := beforModels.GetIdentities()
	primaryKey := beforModels.GetPrimaryKey()
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
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
	primaryKeys, err := GetPrimaryKey(sqlRawEvent.Database, table)
	if err != nil {
		return err
	}

	exp := getIdentityFromWhere(stmt.Where.Expr, primaryKeys[0].Column)

	ids := []string{
		sqlparser.String(exp), // 此处需要再处理，delete 目前使用不到，暂时不写，仅提供思路
	}
	selectSQL := getByIDsSQL(table, primaryKeys[0], ids)
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
	primaryKeys, err := GetPrimaryKey(database, table)
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
			PrimaryKey: primaryKeys[0],
			Table:      table,
			data:       []byte(oneResult.String()),
		}
		sqlModels = append(sqlModels, sqlModel)
	}

	return sqlModels, nil
}

const (
	PrimaryKey_Type_Int = "int"
)

func getByIDsSQL(table string, primaryKey BaseField, ids []string) (sql string) {
	idstr := ""
	switch strings.ToLower(primaryKey.Type) {
	case PrimaryKey_Type_Int:
		idstr = strings.Join(ids, `,`)
	default:
		idstr = fmt.Sprintf("'%s'", strings.Join(ids, `','`))
	}
	if strings.Contains(idstr, ",") {
		sql = fmt.Sprintf("select * from `%s` where `%s` in (%s);", table, primaryKey.Column, idstr)
	} else {
		sql = fmt.Sprintf("select * from `%s` where `%s`=%s;", table, primaryKey.Column, idstr)
	}
	return sql
}
