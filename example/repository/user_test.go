package repository

import (
	"testing"
	"time"
)

func TestCreateBlog(t *testing.T) {
	b := Blog{
		ID:      1,
		UserID:  2,
		Content: "test",
	}
	b.AddBlog()
	time.Sleep(3 * time.Second)
}

func TestUpdateUser(t *testing.T) {
	t.Run("update name", func(t *testing.T) {
		u := User{
			ID:   3,
			Name: "new name",
		}
		u.Update()

	})
	t.Run("not update name", func(t *testing.T) {
		u := User{
			ID: 3,
		}
		u.Update()
	})

	time.Sleep(3 * time.Second)
}
