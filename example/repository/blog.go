package repository

import (
	"fmt"

	"github.com/spf13/cast"
	"github.com/suifengpiao14/autofillcopyfield"
)

func init() {
	new(Blog).initSubscriber()
}

const (
	EVENT_MODEL_NAME_BLOG_CREATED = "blog_created"
)

type Blog struct {
	ID       int    `json:"id"`
	UserID   int    `json:"userId"`
	UserName string `json:"userName"`
	Content  string `json:"content"`
}

func (b Blog) AddBlog() (err error) {
	//todo add blog
	//publish event
	b.emitBlogCreated()
	return nil
}

func (b Blog) initSubscriber() {
	autofillcopyfield.Subscriber(EVENT_MODEL_NAME_BLOG_CREATED, b.handlerBlogCreated)
	autofillcopyfield.Subscriber(EVENT_MODEL_NAME_USER_UPDATED, b.handlerUserUpdated)
}

func (b Blog) handlerUserUpdated(event *autofillcopyfield.Event) (err error) {
	userName := ""
	if !event.NewAttr.GetValue("name", &userName) {
		return
	}
	userID := 0
	event.SourceID.GetValue("id", &userID)
	fmt.Printf("todo: update blog table record's userName to `%s` which userId is %d \n", userName, userID)
	return nil
}

func (b Blog) handlerBlogCreated(event *autofillcopyfield.Event) (err error) {
	blogID := 0
	event.SourceID.GetValue("id", &blogID)
	userID := 0
	event.NewAttr.GetValue("userId", &userID)
	fmt.Printf("todo: update blog table record's userName  which id is %d, and userId is %d \n", blogID, userID)
	return nil
}

func (b Blog) emitBlogCreated() (err error) {
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
