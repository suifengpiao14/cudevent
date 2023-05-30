package repository

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/suifengpiao14/syncdata"
)

func init() {
	new(Blog).initSubscriber()
}

const (
	EVENT_MODEL_NAME_BLOG_CREATED  = "blog_created"
	EVENT_MODEL_NAME_BLOG_CREATING = "blog_creating"
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
	syncdata.Subscriber(EVENT_MODEL_NAME_BLOG_CREATED, b.handlerBlogCreated)
	syncdata.Subscriber(EVENT_MODEL_NAME_USER_UPDATED, b.handlerUserUpdated)
}

func (b Blog) handlerUserUpdated(msg *message.Message) (err error) {
	return nil
}

func (b Blog) handlerBlogCreated(msg *message.Message) (err error) {
	return nil
}

func (b Blog) emitBlogCreated() (err error) {
	payload := syncdata.ChangedPayload{
		EventType: syncdata.EVENT_TYPE_CREATED,
		ID:        "1",
	}
	_ = payload

	// err = syncdata.Publish(EVENT_MODEL_NAME_BLOG_CREATED, payload)
	// if err != nil {
	// 	return err
	// }
	return nil
}
