package repository

import (
	"github.com/suifengpiao14/syncdata"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (u User) Update() {
	//todo 更新数据
	//发起事件
	u.emitUserUpdate()
}

const (
	EVENT_MODEL_NAME_USER_UPDATED = "user_updated"
)

func (u User) emitUserUpdate() (err error) {
	payload := syncdata.ChangedPayload{
		EventType: syncdata.EVENT_TYPE_UPDATED,
		ID:        u.ID,
	}
	if u.Name != "" {
		payload.Before = User{
			Name: "old_name",
		}
		payload.After = User{
			Name: u.Name,
		}

	}

	// err = syncdata.Publish(EVENT_MODEL_NAME_USER_UPDATED, *payload)
	// if err != nil {
	// 	return err
	// }
	return nil
}

// func SubBlogCopyField1(b Blog) (err error) {

// 	event := syncdata.EventWithSub{
// 		Type: syncdata.EVENT_TYPE_CREATED,
// 		Source: syncdata.ProcessMessage{
// 			ProcessName: "sql",
// 			RunContexts: []syncdata.RunContext{
// 				{
// 					Name:   "sql",
// 					Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
// 					Input: syncdata.Fields{
// 						syncdata.Field{Name: "id", Value: b.ID, Type: "int"},
// 					},
// 					Script: "update blog set `user_name`='{{.userName}}' where id={{.id}};",
// 					Dependencies: []syncdata.RunContext{
// 						{
// 							Name:   "sql",
// 							Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
// 							Script: "{{define `getUser`}}select * from user where id={{.id}} {{end}}",
// 							OutputSchema: `
// 							version=http://json-schema.org/draft-07/schema,id=output,direction=out
// 							fullname=username,src=getUserOut.0.name,required
// 							`,
// 							Input: syncdata.Fields{
// 								syncdata.Field{Name: "id", Value: b.UserID, Type: "int"},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// 	_ = event
// 	/* 	_, err = syncdata.Publish(event)
// 	   	if err != nil {
// 	   		return err
// 	   	} */
// 	return nil
// }
