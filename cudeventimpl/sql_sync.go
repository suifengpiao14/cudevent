package cudeventimpl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/sqlexec"
)

// BaseField 平铺的数据库字段结构
type BaseField struct {
	Database   string `json:"database" db:"database"`
	Table      string `json:"table" db:"table"`
	Column     string `json:"column" db:"column"`
	Type       string `json:"type" db:"type"`
	PrimaryKey bool   `json:"primaryKey" db:"primaryKey"`
}

const BaseField_Type_string = "string"

func (bf BaseField) FieldFullname() (fieldFullname string) {
	fieldFullname = fmt.Sprintf("`%s`.`%s`.`%s`", bf.Database, bf.Table, bf.Column)
	return fieldFullname
}

func (bf BaseField) TableFullname() (tableFullname string) {
	tableFullname = fmt.Sprintf("%s.%s", bf.Database, bf.Table)
	return tableFullname
}

func (bf BaseField) isPrimary() (yes bool, err error) {
	primaryKey, err := GetPrimaryKey(bf.Database, bf.Table)
	if err != nil {
		return false, err
	}
	yes = bf.Column == primaryKey.Column
	return yes, nil
}

type FieldRelation struct {
	Scene string `json:"scene"`
	Dst   BaseField
	Src   BaseField
}

const (
	Relation_Scene_Dest_Insert = "dst-insert"
	Relation_Scene_Src_Update  = "src-update"
)

func (fr FieldRelation) SQL() (sqlSegment string) {
	sqlSegment = fmt.Sprintf("%s=%s", fr.Dst.FieldFullname(), fr.Src.FieldFullname())
	return sqlSegment
}

type FieldRelations []FieldRelation

func (frs FieldRelations) GetByScene(scene string) (sameSceneFieldRelations FieldRelations) {
	sameSceneFieldRelations = make(FieldRelations, 0)
	for _, fr := range frs {
		if strings.Contains(fr.Scene, scene) {
			sameSceneFieldRelations = append(sameSceneFieldRelations, fr)
		}
	}
	return sameSceneFieldRelations
}
func (frs *FieldRelations) Add(fieldRelations ...FieldRelation) {
	*frs = append(*frs, fieldRelations...)
	*frs = frs.Uniqueue()
}
func (frs FieldRelations) Uniqueue() (uniqueued FieldRelations) {
	uniqueued = make(FieldRelations, 0)
	m := map[string]struct{}{}

	for _, fr := range frs {
		str := fr.SQL()
		_, exists := m[str]
		if exists {
			continue
		}
		m[str] = struct{}{} // 记录
		uniqueued = append(uniqueued, fr)
	}
	return uniqueued
}
func (frs FieldRelations) GetBySrcTable(srcTable string) (sameSrcTableFieldRelations FieldRelations) {
	sameSrcTableFieldRelations = make(FieldRelations, 0)
	for _, fr := range frs {
		if strings.EqualFold(fr.Src.Table, srcTable) {
			sameSrcTableFieldRelations = append(sameSrcTableFieldRelations, fr)
		}
	}
	return sameSrcTableFieldRelations
}

// SetScene 批量设置场景值
func (frs *FieldRelations) SetScene(scene string) (withSceneTableFieldRelations FieldRelations) {
	for i := range *frs {
		(*frs)[i].Scene = scene
	}
	return *frs
}

func (frs FieldRelations) GetByDstTable(dstTable string) (sameDstTableFieldRelations FieldRelations) {
	sameDstTableFieldRelations = make(FieldRelations, 0)
	for _, fr := range frs {
		if strings.EqualFold(fr.Dst.Table, dstTable) {
			sameDstTableFieldRelations = append(sameDstTableFieldRelations, fr)
		}
	}
	return sameDstTableFieldRelations
}

func (frs FieldRelations) SQL(sep string) (updateSegment string) {
	arr := make([]string, 0)
	for _, fr := range frs {
		arr = append(arr, fr.SQL())
	}
	updateSegment = strings.Join(arr, sep)
	return updateSegment
}

// Tables 获取集合中所有表名，这个地方不能增加db名称，因为集合中有的db为空，后续确实需要支持垮DB操作的建议用领域外事件处理，或者手动书写
func (frs FieldRelations) Tables() (dstTables []string, srcTables []string) {
	srcMap := map[string]struct{}{}
	dstMap := map[string]struct{}{}
	for _, fr := range frs {
		dstMap[fr.Dst.Table] = struct{}{}
		srcMap[fr.Src.Table] = struct{}{}
	}
	// 目标表
	dstTables = make([]string, 0)
	for k := range dstMap {
		dstTables = append(dstTables, k)
	}
	dstSlice := sort.StringSlice(dstTables)
	sort.Sort(dstSlice)
	// 数据源表
	srcTables = make([]string, 0)
	for k := range srcMap {
		srcTables = append(srcTables, k)
	}
	srcSlice := sort.StringSlice(srcTables)
	sort.Sort(srcSlice)

	return dstSlice, srcSlice
}

// SplitPrimaryFieldRelation 筛选出主键关联关系（放到where条件内，并在更新的字段内删除）
func (frs FieldRelations) SplitPrimaryFieldRelation() (ordinaryFieldRelations FieldRelations, primaryFieldRealtions FieldRelations) {
	ordinaryFieldRelations = make(FieldRelations, 0)
	primaryFieldRealtions = make(FieldRelations, 0)
	for _, fr := range frs {
		if fr.Src.PrimaryKey {
			primaryFieldRealtions = append(primaryFieldRealtions, fr)
			continue
		}
		ordinaryFieldRelations = append(ordinaryFieldRelations, fr)
	}
	return ordinaryFieldRelations, primaryFieldRealtions
}

// GetPrimayRelationBySrcTable 获取主键关联关系
func (frs FieldRelations) GetPrimayRelationBySrcTable(srcTable string) (baseField *FieldRelation, err error) {
	for _, fr := range frs {
		if fr.Src.Table == srcTable && fr.Src.PrimaryKey {
			return
		}
	}
	err = errors.Errorf("missing associated fields,got:%s", frs.SQL(","))
	return nil, err
}

var (
	ERROR_NO_UPDATE_FIELD                                  = errors.New("at least one update field required")
	ERROR_PRIMARY_RELATION_FIELD_LESS_THEN_SRC_TABLE_COUNT = errors.New("primary relation field less then src table count,mybe missing some relation field")
)

// SyncRedundantFieldByDstPrimaryKey 通过目标主键更新目标冗余字段(目标记录新增后触发) 获取更新sql语句 模板 通过sql更新语句，同步冗余字段值，该方法配合事件异步更新，有延迟
func (frs FieldRelations) SyncRedundantFieldByDstPrimaryKey() (syncUpdateNamedSql string, err error) {
	filedRelations := frs
	if len(filedRelations) == 0 {
		err = errors.Errorf("SyncRedundantFieldByDstPrimaryKey filedRelations required, got empty")
		return "", err
	}
	database := frs[0].Dst.Database
	dstTables, srcTables := frs.Tables()
	if len(dstTables) != 1 {
		err = errors.Errorf("SyncRedundantFieldBySrcPrimaryKey dst table need only one get more than one :%s", strings.Join(dstTables, ","))
		return "", err
	}
	dstTable := dstTables[0]
	ordinaryFieldRelations, primaryFieldRealtions := filedRelations.SplitPrimaryFieldRelation()
	if len(ordinaryFieldRelations) == 0 {
		err = errors.WithMessagef(ERROR_NO_UPDATE_FIELD, ",got:%s", filedRelations.SQL(","))
		return "", err
	}
	if len(primaryFieldRealtions) < len(srcTables) {
		err = errors.WithMessagef(ERROR_PRIMARY_RELATION_FIELD_LESS_THEN_SRC_TABLE_COUNT, ",got:%s", filedRelations.SQL(","))
		return "", err
	}
	updateSegment := ordinaryFieldRelations.SQL(",")
	dstPrimaryField, err := GetPrimaryKey(database, dstTable)
	if err != nil {
		return "", err
	}
	where := fmt.Sprintf("%s and %s=:ID", primaryFieldRealtions.SQL(" and "), dstPrimaryField.FieldFullname())
	syncUpdateNamedSql = fmt.Sprintf("update %s,%s set %s where 1=1 and %s;", dstTable, strings.Join(srcTables, ","), updateSegment, where)
	return syncUpdateNamedSql, nil
}

// SyncRedundantFieldBySrcPrimaryKey 通过数据源主键更新目标冗余字段(数据源字段更新，如状态更新,此时数据源只能是一个,返回sql有多条——主表一个字段，被多个表冗余) 获取更新sql语句 模板 通过sql更新语句，同步冗余字段值，该方法配合事件异步更新，有延迟
func (frs FieldRelations) SyncRedundantFieldBySrcPrimaryKey() (syncUpdateNamedSqls []string, err error) {
	filedRelations := frs
	syncUpdateNamedSqls = make([]string, 0)
	if len(filedRelations) == 0 {
		err = errors.Errorf("SyncRedundantFieldBySrcPrimaryKey filedRelations required, got empty")
		return nil, err
	}
	dstTables, srcTables := frs.Tables()
	if len(srcTables) != 1 {
		err = errors.Errorf("SyncRedundantFieldBySrcPrimaryKey src table need only one get more than one :%s", strings.Join(srcTables, ","))
		return nil, err
	}
	database := frs[0].Src.Database
	srcTable := srcTables[0]
	for _, dstTable := range dstTables {
		subFieldRelations := frs.GetByDstTable(dstTable)
		ordinaryFieldRelations, primaryFieldRealtions := subFieldRelations.SplitPrimaryFieldRelation()
		if len(ordinaryFieldRelations) == 0 {
			err = errors.WithMessagef(ERROR_NO_UPDATE_FIELD, ",got:%s", subFieldRelations.SQL(","))
			return nil, err
		}
		if len(primaryFieldRealtions) < 1 {
			err = errors.WithMessagef(ERROR_PRIMARY_RELATION_FIELD_LESS_THEN_SRC_TABLE_COUNT, ",got:%s", subFieldRelations.SQL(","))
			return nil, err
		}

		updateSegment := ordinaryFieldRelations.SQL(",")
		srcPrimaryField, err := GetPrimaryKey(database, srcTable)
		if err != nil {
			return nil, err
		}
		where := fmt.Sprintf("%s and %s=:ID", primaryFieldRealtions.SQL(" and "), srcPrimaryField.FieldFullname())
		syncUpdateNamedSql := fmt.Sprintf("update %s,%s set %s where 1=1 and %s;", dstTable, srcTable, updateSegment, where)
		syncUpdateNamedSqls = append(syncUpdateNamedSqls, syncUpdateNamedSql)
	}
	return syncUpdateNamedSqls, nil
}

// ExplainSQLWithID 将只有Id参数的named sql 转为sql
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
		relationStr = funcs.StandardizeSpaces(relationStr)
		relationStr = strings.Trim(relationStr, ",")
		if relationStr == "" {
			continue
		}
		arr := strings.Split(relationStr, ",")
		for _, pair := range arr {
			pair = strings.TrimSpace(pair)
			all = append(all, pair)
		}

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

// ParseField 字符串转BaseField类型
func ParseField(dbTableField string) (baseField *BaseField, err error) {
	baseField = &BaseField{}
	dbTableField = strings.ReplaceAll(dbTableField, "`", "")
	arr := strings.Split(dbTableField, ".")
	l := len(arr)
	if l != 2 && l != 3 {
		err = errors.Errorf("dbTableFiled want [db.]table.filed struct ,got:%s", dbTableField)
		return nil, err
	}
	baseField.Table = arr[0]
	baseField.Column = arr[1]
	yes, err := baseField.isPrimary()
	if err != nil {
		return nil, err
	}
	baseField.PrimaryKey = yes
	return baseField, nil
}
