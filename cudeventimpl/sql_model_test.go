package cudeventimpl_test

import (
	"github.com/suifengpiao14/sqlexec"
	"github.com/suifengpiao14/sshmysql"
)

func GetExecutorSQL() (executorSql *sqlexec.ExecutorSQL) {
	dbConfig := sqlexec.DBConfig{
		DSN: `root:1b03f8b486908bbe34ca2f4a4b91bd1c@mysql(127.0.0.1:3306)/curdservice?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`,
	}
	sshConfig := &sshmysql.SSHConfig{
		Address:        "120.24.156.100:2221",
		User:           "root",
		PriviteKeyFile: "C:\\Users\\Admin\\.ssh\\id_rsa",
		//PriviteKeyFile: "/Users/admin/.ssh/id_rsa",
	}
	executorSql = sqlexec.NewExecutorSQL(dbConfig, sshConfig)
	return executorSql

}
