package cudeventimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/cudevent"
	"github.com/tidwall/gjson"
)

type PrimaryKey struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Type   string `json:"type"`
}

var tablePrimaryKeyMap sync.Map

func RegisterTablePrimaryKey(table string, primaryKey PrimaryKey) {
	tablePrimaryKeyMap.Store(table, &primaryKey)
}

var (
	ERROR_NOT_FOUND_PRIMARY_KEY_BY_TABLE_NAME = errors.New("not found primary key by table name")
	ERROR_INVALID_TYPE                        = errors.New("invalid type")
)

func GetPrimaryKey(table string) (primaryKey *PrimaryKey, err error) {
	v, ok := tablePrimaryKeyMap.Load(table)
	if !ok {
		err = errors.WithMessage(err, table)
		return nil, err
	}
	primaryKey, ok = v.(*PrimaryKey)
	if !ok {
		return nil, ERROR_INVALID_TYPE
	}
	return primaryKey, nil
}

type SQLModel struct {
	PrimaryKey PrimaryKey
	Table      string
	data       []byte
}

func (m SQLModel) GetIdentity() (id string) {
	return gjson.GetBytes(m.data, m.PrimaryKey.Column).String()
}

func (m SQLModel) GetDomain() (domain string) {
	return m.Table
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

func (ms SQLModels) GetPrimaryKey() (primaryKey *PrimaryKey) {
	for _, m := range ms {
		return &m.PrimaryKey
	}
	return nil
}

type DBExecutor interface {
	ExecOrQueryContext(ctx context.Context, sqls string, out interface{}) (err error)
}
type DBExecutorGetter func() (dbExecutor DBExecutor)

type SQLRawEvent struct {
	DBExecutorGetter DBExecutorGetter
	SQL              string `json:"sql"`
	LastInsertId     string `json:"lastInsertId"`
	AffectedRows     string `json:"affectedRows"`
	BeforeData       []byte // update 更新前的数据
}

func PublishSQLRawEvent(sqlRawEvent SQLRawEvent) (err error) {
	if sqlRawEvent.SQL == "" {
		err = errors.New("SQL required")
		return err
	}
	if sqlRawEvent.DBExecutorGetter == nil {
		err = errors.New("DBExecutorGetter required")
		return err
	}
	stmt, _, err := ParseSQL(sqlRawEvent.SQL)
	if err != nil {
		return err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return emitInsertEvent(sqlRawEvent, stmt)
	case *sqlparser.Update:
		return emitUpdatedEvent(sqlRawEvent, stmt)
	case *sqlparser.Delete:
		return emitDeleteEvent(sqlRawEvent, stmt)
	}

	err = errors.New("not souport type")
	return err

}

func PublishSQLRawEventAsync(sqlRawEvent SQLRawEvent) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				_ = rec
				// todo 记录错误
			}
		}()
		err := PublishSQLRawEvent(sqlRawEvent)
		_ = err //todo 记录err

	}()

}

//getIdentityFromWhere 从where 条件中获取主键条件——需完善
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

func emitDeleteEvent(sqlRawEvent SQLRawEvent, stmt *sqlparser.Delete) (err error) {
	table := sqlparser.String(stmt.TableExprs[0])
	primaryKey, err := GetPrimaryKey(table)
	if err != nil {
		return err
	}

	exp := getIdentityFromWhere(stmt.Where.Expr, primaryKey.Column)

	ids := []string{
		sqlparser.String(exp), // 此处需要再处理，delete 目前使用不到，暂时不写，仅提供思路
	}
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	b, err := getData(sqlRawEvent.DBExecutorGetter, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, b)
	if err != nil {
		return err
	}
	err = cudevent.EmitCreatedEvent(afterModels.ToCUDEmiter()...)
	if err != nil {
		return err
	}
	afterModels.ToCUDEmiter()

	return nil
}
func emitInsertEvent(sqlRawEvent SQLRawEvent, stmt *sqlparser.Insert) (err error) {
	table := sqlparser.String(stmt.Table)
	primaryKey, err := GetPrimaryKey(table)
	if err != nil {
		return err
	}
	ids := []string{
		sqlRawEvent.LastInsertId,
	}
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	b, err := getData(sqlRawEvent.DBExecutorGetter, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, b)
	if err != nil {
		return err
	}
	err = cudevent.EmitCreatedEvent(afterModels.ToCUDEmiter()...)
	if err != nil {
		return err
	}
	afterModels.ToCUDEmiter()

	return nil
}

func emitUpdatedEvent(sqlRawEvent SQLRawEvent, stmt *sqlparser.Update) (err error) {

	/* 	selectSQL := ConvertUpdateToSelect(stmt)
	   	err = sqlRawEvent.DBExecutorGetter().ExecOrQueryContext(context.Background(), selectSQL, &out)
	*/
	table := sqlparser.String(stmt.TableExprs[0])
	beforModels, err := byte2SQLModels(table, sqlRawEvent.BeforeData)
	if err != nil {
		return err
	}

	ids := beforModels.GetIdentities()
	primaryKey := beforModels.GetPrimaryKey()
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	b, err := getData(sqlRawEvent.DBExecutorGetter, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, b)
	if err != nil {
		return err
	}
	err = cudevent.EmitUpdatedEvent(beforModels.ToCUDEmiter(), afterModels.ToCUDEmiter())
	if err != nil {
		return err
	}
	return nil
}

func byte2SQLModels(table string, b []byte) (sqlModels SQLModels, err error) {
	primaryKey, err := GetPrimaryKey(table)
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
			PrimaryKey: *primaryKey,
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

func getByIDsSQL(table string, primaryKey PrimaryKey, ids []string) (sql string) {
	idstr := ""
	switch strings.ToLower(primaryKey.Type) {
	case PrimaryKey_Type_Int:
		idstr = strings.Join(ids, `,`)
	default:
		idstr = fmt.Sprintf("'%s'", strings.Join(ids, `','`))
	}

	sql = fmt.Sprintf("select * from `%s` where `%s` in (%s);", table, primaryKey.Column, idstr)
	return sql
}

func getData(dbExecutorGetter DBExecutorGetter, selectSQL string) (b []byte, err error) {
	var out interface{}
	err = dbExecutorGetter().ExecOrQueryContext(context.Background(), selectSQL, &out)
	if err != nil {
		return nil, err
	}
	b, err = json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func ParseSQL(sql string) (stms sqlparser.Statement, typ string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, "", err
	}

	switch stmt.(type) {
	case *sqlparser.Update:
		typ = SQL_TYPE_UPDATE

	case *sqlparser.Insert:
		typ = SQL_TYPE_INSERT
	case *sqlparser.Delete:
		typ = SQL_TYPE_DELETE
	default:
		err = errors.New("unsupported SQL statement type")
	}
	if err != nil {
		return nil, "", err
	}
	return stmt, typ, nil

}

const (
	SQL_TYPE_UPDATE = "update"
	SQL_TYPE_INSERT = "insert"
	SQL_TYPE_DELETE = "delete"
)

func ConvertInsertToSelect(stmt *sqlparser.Insert, primaryKey string, primaryKeyValue string) (selectSQL string) {
	// 获取 INSERT 语句的字段列表
	var selectFields []string
	for _, col := range stmt.Columns {
		selectFields = append(selectFields, sqlparser.String(col))
	}
	// 获取 INSERT 语句的表名
	tableName := sqlparser.String(stmt.Table)
	selectField := strings.Join(selectFields, ", ")
	where := fmt.Sprintf("`%s`=%s", primaryKey, primaryKeyValue)
	selectSQL = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectField, tableName, where)
	return selectSQL
}

func ConvertUpdateToSelect(stmt *sqlparser.Update) (selectSQL string) {
	// 将 UPDATE 语句中的 SET 子句转换为 SELECT 语句的字段列表
	var selectFields []string
	for _, expr := range stmt.Exprs {
		selectFields = append(selectFields, sqlparser.String(expr.Name))
	}
	tableName := sqlparser.String(stmt.TableExprs)
	selectField := strings.Join(selectFields, ", ")
	where := sqlparser.String(stmt.Where)
	selectSQL = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectField, tableName, where)
	return selectSQL
}

func ConvertDeleteToSelect(stmt *sqlparser.Delete) (selectSQL string) {
	// 获取 DELETE 语句的表名
	selectField := "*"
	tableName := sqlparser.String(stmt.TableExprs)
	where := sqlparser.String(stmt.Where)
	selectSQL = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectField, tableName, where)
	return selectSQL
}
