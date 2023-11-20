package cudeventimpl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
	"github.com/suifengpiao14/sqlstream"
	"github.com/suifengpiao14/stream"
)

func GetExecutorSQL() (executorSql *sqlstream.ExecutorSQL) {
	dbConfig := sqlstream.DBConfig{
		DSN: `root:1b03f8b486908bbe34ca2f4a4b91bd1c@mysql(127.0.0.1:3306)/curdservice?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`,
	}
	sshConfig := &sqlstream.SSHConfig{
		Address: "120.24.156.100:2221",
		User:    "root",
		//PriviteKeyFile: "C:\\Users\\Admin\\.ssh\\id_rsa",
		PriviteKeyFile: "/Users/admin/.ssh/id_rsa",
	}
	executorSql = sqlstream.NewExecutorSQL(dbConfig, sshConfig)
	return executorSql

}

func TestCud(t *testing.T) {
	executorSql := GetExecutorSQL()
	err := cudeventimpl.RegisterTablePrimaryKeyByDB(executorSql.GetDB(), "curdservice")
	require.NoError(t, err)
	s := stream.NewStream(nil)
	cudEventPackHandler := cudeventimpl.CUDEventPackHandler(executorSql.GetDB())
	sqlExecPack := sqlstream.MysqlPackHandler(executorSql.GetDB())
	t.Run("select", func(t *testing.T) {
		sql := "select * from service where 1=1;"
		ctx := context.Background()
		var out interface{}
		s.AddPack(sqlstream.Byte2StructPackHandler(&out))
		s.AddPack(cudEventPackHandler)
		s.AddPack(sqlExecPack)
		_, err := s.Run(ctx, []byte(sql))
		require.NoError(t, err)
		fmt.Println(out)
	})

	t.Run("update", func(t *testing.T) {
		sql := "update service set config='{}' where name like 'a1%';"
		ctx := context.Background()
		var out interface{}
		s.AddPack(sqlstream.Byte2StructPackHandler(&out))
		s.AddPack(cudEventPackHandler)
		s.AddPack(sqlExecPack)
		_, err := s.Run(ctx, []byte(sql))
		require.NoError(t, err)
		fmt.Println(out)
	})
	t.Run("insert", func(t *testing.T) {
		sql := "insert into service (name) values('a23');"
		ctx := context.Background()
		var out interface{}
		s.AddPack(sqlstream.Byte2StructPackHandler(&out))
		s.AddPack(cudEventPackHandler)
		s.AddPack(sqlExecPack)
		_, err := s.Run(ctx, []byte(sql))
		require.NoError(t, err)
		fmt.Println(out)
	})
	t.Run("soft delete", func(t *testing.T) {
		sql := fmt.Sprintf("update service set deleted_at='%s' where name like 'a1%%';", time.Now().Format("2006-01-02 15:04:05"))
		ctx := context.Background()
		var out interface{}
		s.AddPack(sqlstream.Byte2StructPackHandler(&out))
		s.AddPack(cudEventPackHandler)
		s.AddPack(sqlExecPack)
		_, err := s.Run(ctx, []byte(sql))
		require.NoError(t, err)
		fmt.Println(out)
	})

}
