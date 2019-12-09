package redigo

import (
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

func ConnByEtcd(dbKey string, endpoints ...string) (pool *redis.Pool, err error) {
	connStr, err := etcd.Conn(endpoints...).Get(dbKey)
	if err != nil {
		return nil, err
	}
	pool, err = connByConnByte(connStr)
	return
}

func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (pool *redis.Pool, err error) {
	connStr, err := etcd.Conn(endpoints...).Auth(etcdName, etcdPass).Get(dbKey)
	if err != nil {
		return nil, err
	}
	pool, err = connByConnByte(connStr)
	return
}

func ConnByEnv(env, dbKey string) (pool *redis.Pool, err error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	pool, err = connByConnByte(connStr)
	return
}

func ConnByStr(host string, port int, password string, dbNumber, maxActive, maxIdle int) (pool *redis.Pool, err error) {
	cfg := new(dbConn)
	cfg.Host = host
	cfg.Port = port
	cfg.Password = password
	cfg.DBNumber = dbNumber
	cfg.MaxActive = maxActive
	cfg.MaxIdle = maxIdle

	pool, err = conn(cfg)
	return
}

func connByConnByte(connByte []byte) (pool *redis.Pool, err error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}

	pool, err = conn(cfg)
	return
}

func conn(cfg *dbConn) (*redis.Pool, error) {
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

	return pool, nil
}
