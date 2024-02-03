package cudeventimpl_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
	"github.com/suifengpiao14/sqlexec"
)

func GetExecutorSQL() (executorSql *sqlexec.ExecutorSQL) {
	dbConfig := sqlexec.DBConfig{
		DSN: `root:1b03f8b486908bbe34ca2f4a4b91bd1c@mysql(127.0.0.1:3306)/curdservice?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`,
	}
	sshConfig := &sqlexec.SSHConfig{
		Address:        "120.24.156.100:2221",
		User:           "root",
		PriviteKeyFile: "C:\\Users\\Admin\\.ssh\\id_rsa",
		//PriviteKeyFile: "/Users/admin/.ssh/id_rsa",
	}
	executorSql = sqlexec.NewExecutorSQL(dbConfig, sshConfig)
	return executorSql

}

func TestCud(t *testing.T) {
	executorSql := GetExecutorSQL()
	err := cudeventimpl.RegisterTablePrimaryKeyByDB(executorSql.GetDB(), "curdservice")
	require.NoError(t, err)

}
