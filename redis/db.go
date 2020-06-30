package redis

import (
	"encoding/json"
	"fmt"
	"github.com/chu108/cmany_db/etcd"
	"github.com/go-redis/redis"
	"time"
)

type dbConn struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
	DBNumber int    `json:"db_number"`
}

/*
通过ETCD方式连接数据库
dbKey etcd存储的数据库连接字符串的key
endpoints etcd的ip节点列表
*/
func ConnByEtcd(dbKey string, endpoints ...string) (*redis.Client, error) {
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
func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (*redis.Client, error) {
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
func ConnByEnv(env, dbKey string) (*redis.Client, error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	return connByConnByte(connStr)
}

/*
以字符串的方式连接数据库
host 主机地址
port 端口
password 密码
*/
func ConnByStr(host string, port int, password string) (client *redis.Client, err error) {
	cfg := new(dbConn)
	cfg.Host = host
	cfg.Port = port
	cfg.Password = password
	cfg.DBNumber = 0
	return conn(cfg)
}

func connByConnByte(connByte []byte) (client *redis.Client, err error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}
	return conn(cfg)
}

func conn(cfg *dbConn) (*redis.Client, error) {
	cli := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:    cfg.Password,
		DB:          cfg.DBNumber,
		IdleTimeout: time.Second * 60,
	})

	_, err := cli.Ping().Result()
	if err != nil {
		return nil, err
	}

	return cli, nil
}
