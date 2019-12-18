package mgo

import (
	"encoding/json"
	"github.com/chu108/cmany_db/etcd"
	"gopkg.in/mgo.v2"
	"time"
)

type dbConn struct {
	Url       string
	PoolLimit int
}

/*
通过ETCD方式连接数据库
dbKey etcd存储的数据库连接字符串的key
endpoints etcd的ip节点列表
*/
func ConnByEtcd(dbKey string, endpoints ...string) (*mgo.Session, error) {
	connStr, err := etcd.Conn(endpoints...).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

/*
通过ETCD 授权方式连接数据库
dbKey etcd存储的数据库连接字符串的key
etcdName etcd用户名
etcdPass etcd密码
endpoints etcd的ip节点列表
*/
func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (*mgo.Session, error) {
	connStr, err := etcd.Conn(endpoints...).Auth(etcdName, etcdPass).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

/*
通过ENV 变量方式连接数据库
env ETCD变量的名称，如ETCD_ADDR=127.0.0.1:2379
dbKey etcd存储的数据库连接字符串的key
*/
func ConnByEnv(env, dbKey string) (*mgo.Session, error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

/*
以字符串的方式连接数据库
url 数据库地址
poolLimit 线程池数
*/
func ConnByStr(url string, poolLimit int) (*mgo.Session, error) {
	cfg := new(dbConn)
	cfg.Url = url
	cfg.PoolLimit = poolLimit
	return conn(cfg)
}

func connByConnByte(connByte []byte) (*mgo.Session, error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}
	return conn(cfg)
}

func conn(cfg *dbConn) (*mgo.Session, error) {
	db, err := mgo.DialWithTimeout(cfg.Url, time.Second*5)
	if err != nil {
		return nil, err
	}
	db.SetPoolLimit(cfg.PoolLimit)
	return db, nil
}
