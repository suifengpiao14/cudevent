package cudeventimpl

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/suifengpiao14/cudevent"
	"github.com/suifengpiao14/sqlexec"
)

// 冗余字段关系池
var sceneSQLPool = make(SceneSQLs, 0)

//RegisterSceneSQL 注册表冗余字段关系
func RegisterSceneSQL(scene string, relationStr string) (err error) {

	relations, err := ParseFieldRelation(relationStr)
	if err != nil {
		return err
	}
	relations.SetScene(scene)
	dstTables, srcTables := relations.Tables()
	for _, table := range dstTables {
		frs := relations.GetByDstTable(table).GetByScene(Relation_Scene_Dest_Insert)
		if len(frs) == 0 {
			continue
		}
		namedSql, err := frs.SyncRedundantFieldByDstPrimaryKey()
		if err != nil {
			return err
		}

		sceneSQL := SceneSQL{
			Table:    table,
			Scene:    Relation_Scene_Dest_Insert,
			NamedSQL: namedSql,
		}
		sceneSQLPool.Add(sceneSQL)
	}

	for _, table := range srcTables {
		frs := relations.GetBySrcTable(table).GetByScene(Relation_Scene_Src_Update)
		if len(frs) == 0 {
			continue
		}
		namedSqls, err := frs.SyncRedundantFieldBySrcPrimaryKey()
		if err != nil {
			return err
		}
		for _, namedSql := range namedSqls {
			sceneSQL := SceneSQL{
				Table:    table,
				Scene:    Relation_Scene_Src_Update,
				NamedSQL: namedSql,
			}
			sceneSQLPool.Add(sceneSQL)
		}
	}

	return nil
}

//GetSceneSQLs 获取注册表
func GetSceneSQLs() (sceneSQLs SceneSQLs) {
	return sceneSQLPool
}

type SceneSQL struct {
	Table    string `json:"table"`
	Scene    string `json:"scene"`    // 场景
	NamedSQL string `json:"namedSql"` // named sql 模板
}

func (ssql SceneSQL) Exec(db *sql.DB, id any) (err error) {
	sql, err := ExplainSQLWithID(ssql.NamedSQL, id)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = sqlexec.ExecOrQueryContext(ctx, db, sql)
	if err != nil {
		return err
	}
	return nil
}

type SceneSQLs []SceneSQL

func (ssqls SceneSQLs) GetByTableScene(table string, scene string) (subSceneSqls SceneSQLs) {
	subSceneSqls = make(SceneSQLs, 0)
	for _, ssql := range ssqls {
		if strings.EqualFold(ssql.Table, table) && strings.EqualFold(ssql.Scene, scene) {
			subSceneSqls = append(subSceneSqls, ssql)
		}
	}
	return subSceneSqls
}

func (ssqls *SceneSQLs) Add(sceneSQLs ...SceneSQL) {
	*ssqls = append(*ssqls, sceneSQLs...)
	*ssqls = ssqls.Uniqueue()
}
func (ssqls SceneSQLs) Uniqueue() (uniqueued SceneSQLs) {
	uniqueued = make(SceneSQLs, 0)
	m := map[string]struct{}{}

	for _, ssql := range ssqls {
		key := fmt.Sprintf("%s_%s", ssql.Table, ssql.Scene)
		_, exists := m[key]
		if exists {
			continue
		}
		m[key] = struct{}{} // 记录
		uniqueued = append(uniqueued, ssql)
	}
	return uniqueued
}

func (ssqls SceneSQLs) Exec(db *sql.DB, id any) (err error) {
	for _, ssql := range ssqls {
		err = ssql.Exec(db, id)
		if err != nil {
			return err
		}
	}
	return nil
}

//SubcribeRedundantField 订阅新增、修改实现，执行数据表冗余字段值的写入和更新
func SubcribeRedundantField(db *sql.DB) {
	ctx := context.Background()
	cudevent.Subscriber(ctx, func(msg *message.Message) (err error) {
		defer func() {
			err = nil // 记录日志错误，返回nil，不然会导致消息无法消费，channel阻塞
		}()
		changedInfo, err := cudevent.ParseMessage(msg)
		if err != nil {
			return err
		}
		table := changedInfo.Table
		switch changedInfo.EventType {
		case cudevent.EVENT_TYPE_CREATED:
			sceneSQLs := GetSceneSQLs().GetByTableScene(table, Relation_Scene_Dest_Insert)
			for _, payload := range changedInfo.Payloads {
				err = sceneSQLs.Exec(db, payload.ID)
				if err != nil {
					return err
				}
			}

		case cudevent.EVENT_TYPE_UPDATED:
			sceneSQLs := GetSceneSQLs().GetByTableScene(table, Relation_Scene_Src_Update)
			if len(sceneSQLs) == 0 {
				return
			}
			for _, payload := range changedInfo.Payloads {
				err = sceneSQLs.Exec(db, payload.ID)
				if err != nil {
					return err
				}
			}

		}
		return nil
	})
}
