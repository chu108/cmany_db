package mysql

import (
	"database/sql"
	"encoding/json"
	"github.com/chu108/cmany_db/etcd"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type dbConn struct {
	DSN     string `json:"dsn"`
	MaxOpen int    `json:"max_open"`
	MaxIdle int    `json:"max_idle"`
}

type mysqlConfig struct {
	Master dbConn `json:"master"`
	Slave  dbConn `json:"slave"`
}

/*
通过ETCD方式连接数据库
dbKey etcd存储的数据库连接字符串的key
endpoints etcd的ip节点列表
*/
func ConnByEtcd(dbKey string, endpoints ...string) (masterDB, slaveDB *sql.DB, err error) {
	connStr, err := etcd.Conn(endpoints...).Get(dbKey)
	if err != nil {
		return nil, nil, err
	}
	masterDB, slaveDB, err = connByConnByte(connStr)
	return
}

/*
通过ETCD 授权方式连接数据库
dbKey etcd存储的数据库连接字符串的key
etcdName etcd用户名
etcdPass etcd密码
endpoints etcd的ip节点列表
*/
func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (masterDB *sql.DB, slaveDB *sql.DB, err error) {
	connStr, err := etcd.Conn(endpoints...).Auth(etcdName, etcdPass).Get(dbKey)
	if err != nil {
		return nil, nil, err
	}
	masterDB, slaveDB, err = connByConnByte(connStr)
	return
}

/*
通过ENV 变量方式连接数据库
env ETCD变量的名称，如ETCD_ADDR=127.0.0.1:2379
dbKey etcd存储的数据库连接字符串的key
*/
func ConnByEnv(env, dbKey string) (masterDB *sql.DB, slaveDB *sql.DB, err error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, nil, err
	}
	masterDB, slaveDB, err = connByConnByte(connStr)
	return
}

/*
以字符串的方式连接数据库
dsn 数据库连接DSN
maxOpen 最大打开连接
maxIdle 最大闲置的连接数
*/
func ConnByStr(dsn string, maxOpen, maxIdle int) (masterDB, slaveDB *sql.DB, err error) {
	cfg := new(mysqlConfig)
	cfg.Master.DSN = dsn
	cfg.Master.MaxOpen = maxOpen
	cfg.Master.MaxIdle = maxIdle
	cfg.Slave = cfg.Master

	masterDB, slaveDB, err = conn(cfg)
	return
}

func connByConnByte(connByte []byte) (masterDB, slaveDB *sql.DB, err error) {
	cfg := new(mysqlConfig)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, nil, err
	}

	masterDB, slaveDB, err = conn(cfg)
	return
}

func conn(cfg *mysqlConfig) (masterDB, slaveDB *sql.DB, err error) {
	//主库
	masterDB, err = sql.Open("mysql", cfg.Master.DSN)
	if err != nil {
		return nil, nil, err
	}
	masterDB.SetMaxOpenConns(cfg.Master.MaxOpen)
	masterDB.SetMaxIdleConns(cfg.Master.MaxIdle)
	masterDB.SetConnMaxLifetime(time.Second * 100)
	if err = masterDB.Ping(); err != nil {
		return nil, nil, err
	}

	if cfg.Master.DSN == cfg.Slave.DSN {
		slaveDB = masterDB
	}

	//从库
	slaveDB, err = sql.Open("mysql", cfg.Slave.DSN)
	if err != nil {
		return nil, nil, err
	}
	slaveDB.SetMaxOpenConns(cfg.Slave.MaxOpen)
	slaveDB.SetMaxIdleConns(cfg.Slave.MaxIdle)
	slaveDB.SetConnMaxLifetime(time.Second * 100)
	if err = slaveDB.Ping(); err != nil {
		return nil, nil, err
	}

	return
}
