package repository

import (
	"fmt"

	"github.com/suifengpiao14/autofillcopyfield"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (u User) Update() {
	//todo 更新数据
	//发起事件
	EmitUserUpdate(u)
}

const (
	EVENT_MODEL_NAME_USER = "user"
	EVENT_MODEL_NAME_BLOG = "blog"
)

func EmitUserUpdate(u User) (err error) {
	event := autofillcopyfield.Event{
		ModelName: EVENT_MODEL_NAME_USER,
		Type:      autofillcopyfield.EVENT_TYPE_UPDATED,
		SourceID: autofillcopyfield.Fields{
			{Name: "id", Value: "1", Type: "int"},
			{Name: "name", Value: "new_name", Type: "string"},
		},
		OldAttr: autofillcopyfield.Fields{
			{Name: "name", Value: "old_name", Type: "string"},
		},
		NewAttr: autofillcopyfield.Fields{
			{Name: "name", Value: "new_name", Type: "string"},
		},
	}
	_, err = autofillcopyfield.Publish(event)
	if err != nil {
		return err
	}
	return nil
}

func EmitBlogCreated(b Blog) (err error) {
	event := autofillcopyfield.Event{
		ModelName: EVENT_MODEL_NAME_BLOG,
		Type:      autofillcopyfield.EVENT_TYPE_CREATED,
		SourceID: autofillcopyfield.Fields{
			{Name: "id", Value: "1", Type: "int"},
			{Name: "name", Value: "new_name", Type: "string"},
		},
	}
	_, err = autofillcopyfield.Publish(event)
	if err != nil {
		return err
	}
	return nil
}

type Blog struct {
	ID       string `json:"id"`
	UserID   string `json:"userId"`
	UserName string `json:"userName"`
	Content  string `json:"content"`
}

func AddBlog(b Blog) (err error) {
	//todo add blog
	//publish event
	EmitBlogCreated(b)
	return nil
}

func SubBlogCopyField1(event autofillcopyfield.Event) {
	interestedEvents := []string{
		fmt.Sprintf("%s_%s", EVENT_MODEL_NAME_USER, autofillcopyfield.EVENT_TYPE_UPDATED),
		fmt.Sprintf("%s_%s", EVENT_MODEL_NAME_BLOG, autofillcopyfield.EVENT_TYPE_CREATED),
	}
	eventID := event.GetIdentify()
	interested := false
	for _, id := range interestedEvents {
		if id == eventID {
			interested = true
			break
		}
	}
	if !interested {
		return
	}
	//todo 响应事件

}

func SubBlogCopyField(b Blog) (err error) {

	event := autofillcopyfield.EventWithSub{
		Type: autofillcopyfield.EVENT_TYPE_CREATED,
		Source: autofillcopyfield.ProcessMessage{
			ProcessName: "sql",
			RunContexts: []autofillcopyfield.RunContext{
				{
					Name:   "sql",
					Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
					Input: autofillcopyfield.Fields{
						autofillcopyfield.Field{Name: "id", Value: b.ID, Type: "int"},
					},
					Script: "update blog set `user_name`='{{.userName}}' where id={{.id}};",
					Dependencies: []autofillcopyfield.RunContext{
						{
							Name:   "sql",
							Config: `{"dsn":"root:1b03f8b486908bbe34ca2f4a4b91bd1c@ssh(127.0.0.1:3306)/ad?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"}`,
							Script: "{{define `getUser`}}select * from user where id={{.id}} {{end}}",
							OutputSchema: `
							version=http://json-schema.org/draft-07/schema,id=output,direction=out
							fullname=username,src=getUserOut.0.name,required
							`,
							Input: autofillcopyfield.Fields{
								autofillcopyfield.Field{Name: "id", Value: b.UserID, Type: "int"},
							},
						},
					},
				},
			},
		},
	}
	_ = event
	/* 	_, err = autofillcopyfield.Publish(event)
	   	if err != nil {
	   		return err
	   	} */
	return nil
}
