package repository

import (
	"testing"
	"time"
)

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
