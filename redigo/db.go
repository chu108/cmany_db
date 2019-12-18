package redigo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chu108/cmany_db/etcd"
	"github.com/garyburd/redigo/redis"
	"time"
)

type dbConn struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Password  string `json:"password"`
	DBNumber  int    `json:"db_number"`
	MaxActive int    `json:"max_active"`
	MaxIdle   int    `json:"max_idle"`
}

/*
通过ETCD方式连接数据库
dbKey etcd存储的数据库连接字符串的key
endpoints etcd的ip节点列表
*/
func ConnByEtcd(dbKey string, endpoints ...string) (redis.Conn, error) {
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
func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (redis.Conn, error) {
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
func ConnByEnv(env, dbKey string) (redis.Conn, error) {
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
func ConnByStr(host string, port int, password string) (redis.Conn, error) {
	cfg := new(dbConn)
	cfg.Host = host
	cfg.Port = port
	cfg.Password = password
	cfg.DBNumber = 0
	cfg.MaxActive = 100
	cfg.MaxIdle = 10

	return conn(cfg)
}

func connByConnByte(connByte []byte) (redis.Conn, error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}

	return conn(cfg)
}

func conn(cfg *dbConn) (redis.Conn, error) {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				"tcp",
				fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
				redis.DialPassword(cfg.Password),
				redis.DialDatabase(cfg.DBNumber),
				redis.DialConnectTimeout(time.Second*2),
				redis.DialReadTimeout(time.Second*2),
				redis.DialWriteTimeout(time.Second*2),
			)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:     cfg.MaxIdle,   //最大空闲连接数，即会有这么多个连接提前等待着，但过了超时时间也会关闭
		MaxActive:   cfg.MaxActive, //最大连接数，即最多的tcp连接数，一般建议往大的配置，但不要超过操作系统文件句柄个数（centos下可以ulimit -n查看）
		IdleTimeout: time.Second,   //空闲连接超时时间，但应该设置比redis服务器超时时间短。否则服务端超时了，客户端保持着连接也没用
		Wait:        true,          //当超过最大连接数 是报错还是等待，true 等待 false 报错
	}

	return pool.GetContext(context.Background())
}
