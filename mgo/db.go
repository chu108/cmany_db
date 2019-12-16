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

func ConnByEtcd(dbKey string, endpoints ...string) (*mgo.Session, error) {
	connStr, err := etcd.Conn(endpoints...).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (*mgo.Session, error) {
	connStr, err := etcd.Conn(endpoints...).Auth(etcdName, etcdPass).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

func ConnByEnv(env, dbKey string) (*mgo.Session, error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

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
