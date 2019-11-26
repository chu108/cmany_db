package mysql

import (
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type mysqlConfig struct {
	Master struct {
		DSN     string `json:"dsn"`
		MaxOpen int    `json:"max_open"`
		MaxIdle int    `json:"max_idle"`
	} `json:"master"`
	Slave struct {
		DSN     string `json:"dsn"`
		MaxOpen int    `json:"max_open"`
		MaxIdle int    `json:"max_idle"`
	} `json:"slave"`
}

func ConnByEtcd(dbKey string, endpoints ...string) (masterDB *sql.DB, slaveDB *sql.DB, err error) {
	//connStr, err := etcd.
	//if err != nil {
	//	panic(err)
	//}
	return nil, nil, nil
}

func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (masterDB *sql.DB, slaveDB *sql.DB, err error) {
	//connStr, err := etcd.
	//if err != nil {
	//	panic(err)
	//}
	return nil, nil, nil
}

func ConnByEnv(dbKey string) (masterDB *sql.DB, slaveDB *sql.DB, err error) {

	return nil, nil, nil
}

func conn(connStr []byte) {
	var masterDB, slaveDB *sql.DB
	var err error

	cfg := new(mysqlConfig)
	if err := json.Unmarshal(connStr, cfg); err != nil {
		panic(err)
	}

	//主库
	masterDB, err = sql.Open("mysql", cfg.Master.DSN)
	if err != nil {
		panic(err)
	}
	masterDB.SetMaxOpenConns(cfg.Master.MaxOpen)
	masterDB.SetMaxIdleConns(cfg.Master.MaxIdle)
	masterDB.SetConnMaxLifetime(time.Second * 100)
	if err = masterDB.Ping(); err != nil {
		panic(err)
	}

	if cfg.Master.DSN == cfg.Slave.DSN {
		slaveDB = masterDB
	}

	//从库
	slaveDB, err = sql.Open("mysql", cfg.Slave.DSN)
	if err != nil {
		panic(err)
	}
	slaveDB.SetMaxOpenConns(cfg.Slave.MaxOpen)
	slaveDB.SetMaxIdleConns(cfg.Slave.MaxIdle)
	slaveDB.SetConnMaxLifetime(time.Second * 100)
	if err = slaveDB.Ping(); err != nil {
		panic(err)
	}

}
