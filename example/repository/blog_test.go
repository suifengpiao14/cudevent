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
