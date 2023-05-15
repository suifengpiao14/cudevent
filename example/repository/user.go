package repository

import (
	"fmt"

	"github.com/spf13/cast"
	"github.com/suifengpiao14/autofillcopyfield"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (u User) Update() {
	//todo 更新数据
	//发起事件
	EmitUserUpdate(u)
}

const (
	EVENT_MODEL_NAME_USER_UPDATED = "user_updated"
	EVENT_MODEL_NAME_BLOG_CREATED = "blog_created"
)

func EmitUserUpdate(u User) (err error) {
	event := autofillcopyfield.Event{
		Topic:   EVENT_MODEL_NAME_USER_UPDATED,
		EventID: EVENT_MODEL_NAME_USER_UPDATED,
		Type:    autofillcopyfield.EVENT_TYPE_UPDATED,
		SourceID: autofillcopyfield.Fields{
			{Name: "id", Value: cast.ToString(u.ID), Type: "int"},
		},
	}
	if u.Name != "" {
		event.OldAttr = autofillcopyfield.Fields{
			{Name: "name", Value: "old_name", Type: "string"},
		}
		event.NewAttr = autofillcopyfield.Fields{
			{Name: "name", Value: u.Name, Type: "string"},
		}
	}
	_, err = autofillcopyfield.Publish(event)
	if err != nil {
		return err
	}
	return nil
}

func EmitBlogCreated(b Blog) (err error) {
	event := autofillcopyfield.Event{
		Topic:   EVENT_MODEL_NAME_BLOG_CREATED,
		EventID: EVENT_MODEL_NAME_BLOG_CREATED,
		Type:    autofillcopyfield.EVENT_TYPE_CREATED,
		SourceID: autofillcopyfield.Fields{
			{Name: "id", Value: "1", Type: "int"},
		},
		NewAttr: autofillcopyfield.Fields{
			{Name: "userId", Value: cast.ToString(b.UserID), Type: "int"},
		},
	}
	_, err = autofillcopyfield.Publish(event)
	if err != nil {
		return err
	}
	return nil
}

type Blog struct {
	ID       int    `json:"id"`
	UserID   int    `json:"userId"`
	UserName string `json:"userName"`
	Content  string `json:"content"`
}

func init() {
	autofillcopyfield.Subscriber(EVENT_MODEL_NAME_USER_UPDATED, HandlerUserUpdated)
	autofillcopyfield.Subscriber(EVENT_MODEL_NAME_BLOG_CREATED, HandlerBlogCreated)
}
func HandlerUserUpdated(event *autofillcopyfield.Event) (err error) {
	userName := ""
	if !event.NewAttr.GetValue("name", &userName) {
		return
	}
	userID := 0
	event.SourceID.GetValue("id", &userID)
	fmt.Printf("todo: update blog table record's userName to `%s` which userId is %d \n", userName, userID)
	return nil
}
func HandlerBlogCreated(event *autofillcopyfield.Event) (err error) {
	blogID := 0
	event.SourceID.GetValue("id", &blogID)
	userID := 0
	event.NewAttr.GetValue("userId", &userID)
	fmt.Printf("todo: update blog table record's userName  which id is %d, and userId is %d \n", blogID, userID)
	return nil
}

func (b Blog) AddBlog() (err error) {
	//todo add blog
	//publish event
	EmitBlogCreated(b)
	return nil
}

// func SubBlogCopyField1(b Blog) (err error) {

// 	event := autofillcopyfield.EventWithSub{
// 		Type: autofillcopyfield.EVENT_TYPE_CREATED,
// 		Source: autofillcopyfield.ProcessMessage{
// 			ProcessName: "sql",
// 			RunContexts: []autofillcopyfield.RunContext{
// 				{
// 					Name:   "sql",
// 					Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
// 					Input: autofillcopyfield.Fields{
// 						autofillcopyfield.Field{Name: "id", Value: b.ID, Type: "int"},
// 					},
// 					Script: "update blog set `user_name`='{{.userName}}' where id={{.id}};",
// 					Dependencies: []autofillcopyfield.RunContext{
// 						{
// 							Name:   "sql",
// 							Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
// 							Script: "{{define `getUser`}}select * from user where id={{.id}} {{end}}",
// 							OutputSchema: `
// 							version=http://json-schema.org/draft-07/schema,id=output,direction=out
// 							fullname=username,src=getUserOut.0.name,required
// 							`,
// 							Input: autofillcopyfield.Fields{
// 								autofillcopyfield.Field{Name: "id", Value: b.UserID, Type: "int"},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// 	_ = event
// 	/* 	_, err = autofillcopyfield.Publish(event)
// 	   	if err != nil {
// 	   		return err
// 	   	} */
// 	return nil
// }
