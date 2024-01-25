package cudeventimpl

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/sqlexec"
)

//BaseField 平铺的数据库字段结构
type BaseField struct {
	DB         string `json:"db"`
	Table      string `json:"table"`
	Name       string `json:"column"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primaryKey"`
}

const BaseField_Type_string = "string"

func (bf BaseField) FieldFullname() (fieldFullname string) {
	fieldFullname = fmt.Sprintf("`%s`.`%s`", bf.Table, bf.Name)
	if bf.DB != "" {
		fieldFullname = fmt.Sprintf("`%s`.%s", bf.DB, fieldFullname)
	}
	return fieldFullname
}

func (bf BaseField) TableFullname() (tableFullname string) {
	tableFullname = bf.Table
	if bf.DB != "" {
		tableFullname = fmt.Sprintf("`%s`.", bf.DB)
	}
	return tableFullname
}

func (bf BaseField) isPrimary() (yes bool) {
	primaryKey, err := GetPrimaryKey(bf.Table)
	if err != nil {
		return false
	}
	yes = bf.Name == primaryKey.Name
	return yes
}

type FieldRelation struct {
	Dst BaseField
	Src BaseField
}

func (fr FieldRelation) SQL() (sqlSegment string) {
	sqlSegment = fmt.Sprintf("%s=%s", fr.Dst.FieldFullname(), fr.Src.FieldFullname())
	return sqlSegment
}

type FieldRelations []FieldRelation

func (frs FieldRelations) SQL() (updateSegment string) {
	arr := make([]string, 0)
	for _, fr := range frs {
		arr = append(arr, fr.SQL())
	}
	updateSegment = strings.Join(arr, ",")
	return updateSegment
}

//SplitPrimaryFieldRelation 筛选出主键关联关系（放到where条件内，并在更新的字段内删除）
func (frs FieldRelations) SplitPrimaryFieldRelation() (ordinaryFieldRelations FieldRelations, primaryFieldRealtion *FieldRelation) {
	ordinaryFieldRelations = make(FieldRelations, 0)
	primaryFieldRealtion = &FieldRelation{}
	for _, fr := range frs {
		if fr.Src.isPrimary() {
			primaryFieldRealtion = &fr
			continue
		}
		ordinaryFieldRelations = append(ordinaryFieldRelations, fr)
	}
	if primaryFieldRealtion.Src.Name == "" {
		primaryFieldRealtion = nil // 如果为找到，直接返回nil，方便外部判断
	}
	return ordinaryFieldRelations, primaryFieldRealtion
}

//GetPrimayRelationBySrcTable 获取主键关联关系
func (frs FieldRelations) GetPrimayRelationBySrcTable(srcTable string) (baseField *FieldRelation, err error) {
	for _, fr := range frs {
		if fr.Src.Table == srcTable && fr.Src.PrimaryKey {
			return
		}
	}
	err = errors.Errorf("missing associated fields,got:%s", frs.SQL())
	return nil, err
}

var (
	ERROR_NO_UPDATE_FIELD           = errors.New("at least one update field required")
	ERROR_NO_PRIMARY_RELATION_FIELD = errors.New("primary relation field required")
)

// SyncUpdateNamedSQL 获取更新sql语句 模板 通过sql更新语句，同步冗余字段值，该方法配合事件异步更新，有延迟
func (frs FieldRelations) SyncUpdateNamedSQL() (syncUpdateNamedSql string, err error) {
	filedRelations := frs
	if len(filedRelations) == 0 {
		err = errors.Errorf("SyncDataByUpdate filedRelations required, got empty")
		return "", err
	}
	firstFiledRelation := filedRelations[0]
	srcTable := firstFiledRelation.Src.TableFullname()
	dstTable := firstFiledRelation.Dst.TableFullname()
	ordinaryFieldRelations, primaryFieldRealtion := filedRelations.SplitPrimaryFieldRelation()
	if len(ordinaryFieldRelations) == 0 {
		err = errors.WithMessagef(ERROR_NO_UPDATE_FIELD, ",got:%s", filedRelations.SQL())
		return "", err
	}
	if primaryFieldRealtion == nil {
		err = errors.WithMessagef(ERROR_NO_PRIMARY_RELATION_FIELD, ",got:%s", filedRelations.SQL())
		return "", err
	}
	updateSegment := ordinaryFieldRelations.SQL()
	dstPrimaryField, err := GetPrimaryKey(dstTable)
	if err != nil {
		return "", err
	}
	if firstFiledRelation.Dst.DB == "" {
		dstPrimaryField.DB = "" //不需要携带db名称时，情况主键db名称
	}
	where := fmt.Sprintf("%s and %s=:ID", primaryFieldRealtion.SQL(), dstPrimaryField.FieldFullname())
	syncUpdateNamedSql = fmt.Sprintf("update %s,%s set %s where  1=1 and %s limit 1", srcTable, dstTable, updateSegment, where)
	return syncUpdateNamedSql, nil
}

//ExplainSQLWithID 将只有Id参数的named sql 转为sql
func ExplainSQLWithID(namedSql string, id any) (sql string, err error) {
	namedData := map[string]any{
		"ID": id,
		"Id": id,
		"id": id,
	}
	return sqlexec.ExplainSQL(namedSql, namedData)
}

func ParseFieldRelation(relationStrs ...string) (filedRelations FieldRelations, err error) {
	all := make([]string, 0)
	for _, relationStr := range relationStrs {
		if relationStr == "" {
			continue
		}
		all = append(all, strings.Split(relationStr, ",")...)
	}
	filedRelations = make(FieldRelations, 0)
	for _, pairStr := range all {
		pair := strings.Split(pairStr, "=")
		if len(pair) != 2 {
			err = errors.Errorf("ParseFieldRelation want format [dstDb.]dstTable.dstField=[srcDb.]srcTable.srcField ,got %s", pairStr)
			return nil, err
		}
		relation := FieldRelation{}
		dstFieldPath, err := ParseField(pair[0])
		if err != nil {
			return nil, err
		}
		relation.Dst = *dstFieldPath
		srcFieldPath, err := ParseField(pair[1])
		if err != nil {
			return nil, err
		}
		relation.Src = *srcFieldPath
		filedRelations = append(filedRelations, relation)
	}
	return filedRelations, nil
}

//ParseField 字符串转BaseField类型
func ParseField(dbTableField string) (baseField *BaseField, err error) {
	baseField = &BaseField{}
	dbTableField = strings.ReplaceAll(dbTableField, "`", "")
	arr := strings.Split(dbTableField, ".")
	l := len(arr)
	if l != 2 && l != 3 {
		err = errors.Errorf("dbTableFiled want [db.]table.filed struct ,got:%s", dbTableField)
		return nil, err
	}
	if l == 3 {
		baseField.DB = arr[0]
		arr = arr[1:]
	}
	baseField.Table = arr[0]
	baseField.Name = arr[1]
	baseField.PrimaryKey = baseField.isPrimary()
	return baseField, nil
}
