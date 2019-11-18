package mysql

import (
	"cetcd"
	"database/sql"
	"encoding/json"
	"time"
)

var (
	MasterDB *sql.DB
	SlaveDB  *sql.DB
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

func init() {
	connStr, err := cetcd.Get("root/database/mysql/master")
	if err != nil {
		panic(err)
	}

	cfg := new(mysqlConfig)
	if err := json.Unmarshal(connStr, cfg); err != nil {
		panic(err)
	}

	//主库
	MasterDB, err = sql.Open("mysql", cfg.Master.DSN)
	if err != nil {
		panic(err)
	}
	MasterDB.SetMaxOpenConns(cfg.Master.MaxOpen)
	MasterDB.SetMaxIdleConns(cfg.Master.MaxIdle)
	MasterDB.SetConnMaxLifetime(time.Second * 100)
	if err = MasterDB.Ping(); err != nil {
		panic(err)
	}

	if cfg.Master.DSN == cfg.Slave.DSN {
		SlaveDB = MasterDB
	}

	//从库
	SlaveDB, err = sql.Open("mysql", cfg.Slave.DSN)
	if err != nil {
		panic(err)
	}
	SlaveDB.SetMaxOpenConns(cfg.Slave.MaxOpen)
	SlaveDB.SetMaxIdleConns(cfg.Slave.MaxIdle)
	SlaveDB.SetConnMaxLifetime(time.Second * 100)
	if err = SlaveDB.Ping(); err != nil {
		panic(err)
	}

}
