package mongodb

import (
	"context"
	"encoding/json"
	"github.com/chu108/cmany_db/etcd"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type dbConn struct {
	Url    string
	DbName string
}

/*
通过ETCD方式连接数据库
dbKey etcd存储的数据库连接字符串的key
endpoints etcd的ip节点列表
*/
func ConnByEtcd(dbKey string, endpoints ...string) (*mongo.Database, error) {
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
func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (*mongo.Database, error) {
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
func ConnByEnv(env, dbKey string) (*mongo.Database, error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

/*
以字符串的方式连接数据库
url 地址
dbName 数据库名称
*/
func ConnByStr(url, dbName string) (*mongo.Database, error) {
	cfg := new(dbConn)
	cfg.Url = url
	cfg.DbName = dbName
	return conn(cfg)
}

func connByConnByte(connByte []byte) (*mongo.Database, error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}
	return conn(cfg)
}

func conn(cfg *dbConn) (*mongo.Database, error) {
	ctx := Ctx(5)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.Url))
	if err != nil {
		return nil, err
	}
	//是否连接上了数据库
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}
	//设置数据库
	return client.Database(cfg.DbName), nil
}

func CtxAndCancel(timeout int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
}

func Ctx(timeout int) context.Context {
	ctx, _ := CtxAndCancel(timeout)
	return ctx
}
