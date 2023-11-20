package cudeventimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/cast"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/cudevent"
	"github.com/suifengpiao14/sqlstream"
	"github.com/suifengpiao14/stream"
	"github.com/tidwall/gjson"
)

func CUDEventPackHandler(db *sql.DB) (packHandler stream.PackHandler) {
	sqlRawEvent := &SQLRawEvent{}
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			return nil, err
		}
		sqlRawEvent.SQL = sql
		sqlRawEvent.DB = db
		sqlRawEvent.stmt = stmt
		switch stmt := stmt.(type) {
		case *sqlparser.Update: // 更新类型，先查询更新前数据，并保存
			selectSQL := ConvertUpdateToSelect(stmt)
			before, err := sqlstream.QueryContext(ctx, db, selectSQL)
			if err != nil {
				return nil, err
			}
			sqlRawEvent.BeforeData = before
		}
		return input, nil
	}, func(ctx context.Context, input []byte) (out []byte, err error) {
		stmt := sqlRawEvent.stmt
		switch stmt.(type) {
		case *sqlparser.Insert:
			sqlRawEvent.LastInsertId = string(input)
		case *sqlparser.Update:
			sqlRawEvent.RowsAffected = cast.ToInt64(string(input))
		}
		err = PublishSQLRawEvent(sqlRawEvent)
		if err != nil {
			return nil, err
		}
		return input, nil
	})
	return packHandler
}

var SoftDeleteColumn = "deleted_at" // 当update语句出现该列时，当成删除操作

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
		err = errors.WithMessagef(ERROR_NOT_FOUND_PRIMARY_KEY_BY_TABLE_NAME, "%s", table)
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

func (ms SQLModels) GetPrimaryKey() (primaryKey *PrimaryKey) {
	for _, m := range ms {
		return &m.PrimaryKey
	}
	return nil
}

type SQLRawEvent struct {
	stmt         sqlparser.Statement
	DB           *sql.DB
	SQL          string `json:"sql"`
	LastInsertId string `json:"lastInsertId"`
	RowsAffected int64  `json:"affectedRows"`
	BeforeData   string // update 更新前的数据
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
	primaryKey, err := GetPrimaryKey(table)
	if err != nil {
		return err
	}
	var ids []string
	err = json.Unmarshal([]byte(sqlRawEvent.LastInsertId), &ids)
	if err != nil {
		return err
	}
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	ctx := context.Background()
	data, err := sqlstream.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, []byte(data))
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
	/* 	selectSQL := ConvertUpdateToSelect(stmt)
	   	err = sqlRawEvent.DBExecutorGetter().ExecOrQueryContext(context.Background(), selectSQL, &out)
	*/
	table := sqlparser.String(stmt.TableExprs[0])
	beforModels, err := byte2SQLModels(table, []byte(sqlRawEvent.BeforeData))
	if err != nil {
		return err
	}

	ids := beforModels.GetIdentities()
	primaryKey := beforModels.GetPrimaryKey()
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	ctx := context.Background()
	data, err := sqlstream.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, []byte(data))
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
	primaryKey, err := GetPrimaryKey(table)
	if err != nil {
		return err
	}

	exp := getIdentityFromWhere(stmt.Where.Expr, primaryKey.Column)

	ids := []string{
		sqlparser.String(exp), // 此处需要再处理，delete 目前使用不到，暂时不写，仅提供思路
	}
	selectSQL := getByIDsSQL(table, *primaryKey, ids)
	ctx := context.Background()
	data, err := sqlstream.QueryContext(ctx, sqlRawEvent.DB, selectSQL)
	if err != nil {
		return err
	}
	afterModels, err := byte2SQLModels(table, []byte(data))
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
	beforModels, err := byte2SQLModels(table, []byte(sqlRawEvent.BeforeData))
	if err != nil {
		return err
	}
	err = cudevent.EmitDeletedEvent(beforModels.ToCUDEmiter())
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
	if strings.Contains(idstr, ",") {
		sql = fmt.Sprintf("select * from `%s` where `%s` in (%s);", table, primaryKey.Column, idstr)
	} else {
		sql = fmt.Sprintf("select * from `%s` where `%s`=%s;", table, primaryKey.Column, idstr)
	}
	return sql
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
	// var selectFields []string
	// 缺少id
	// for _, expr := range stmt.Exprs {
	// 	selectFields = append(selectFields, sqlparser.String(expr.Name))
	// }
	tableName := sqlparser.String(stmt.TableExprs)
	//selectField := strings.Join(selectFields, ", ") //缺少Id，暂时用*代替
	where := sqlparser.String(stmt.Where)
	selectSQL = fmt.Sprintf("SELECT * FROM %s  %s", tableName, where)
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

func RegisterTablePrimaryKeyByDB(db *sql.DB, dbName string) (err error) {
	sql := fmt.Sprintf("SELECT  table_name `table`,column_name `column`,data_type `type` FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND COLUMN_KEY = 'PRI'", dbName)
	primaryKeys := make([]PrimaryKey, 0)
	rows, err := db.QueryContext(context.Background(), sql)
	if err != nil {
		return err
	}
	err = sqlx.StructScan(rows, &primaryKeys)
	if err != nil {
		return err
	}
	for _, primaryKey := range primaryKeys {
		RegisterTablePrimaryKey(primaryKey.Table, primaryKey)
	}
	return nil
}