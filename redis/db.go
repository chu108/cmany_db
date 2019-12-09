package redis

import (
	"encoding/json"
	"fmt"
	"github.com/chu108/cmany_db/etcd"
	"github.com/go-redis/redis"
)

type dbConn struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
	DBNumber int    `json:"db_number"`
}

func ConnByEtcd(dbKey string, endpoints ...string) (client *redis.Client, err error) {
	connStr, err := etcd.Conn(endpoints...).Get(dbKey)
	if err != nil {
		return nil, err
	}
	client, err = connByConnByte(connStr)
	return
}

func ConnByEtcdAuth(dbKey, etcdName, etcdPass string, endpoints ...string) (client *redis.Client, err error) {
	connStr, err := etcd.Conn(endpoints...).Auth(etcdName, etcdPass).Get(dbKey)
	if err != nil {
		return nil, err
	}
	client, err = connByConnByte(connStr)
	return
}

func ConnByEnv(env, dbKey string) (client *redis.Client, err error) {
	connStr, err := etcd.ConnByEnv(env).Get(dbKey)
	if err != nil {
		return nil, err
	}
	client, err = connByConnByte(connStr)
	return
}

func ConnByStr(host string, port int, password string, dbNumber int) (client *redis.Client, err error) {
	cfg := new(dbConn)
	cfg.Host = host
	cfg.Port = port
	cfg.Password = password
	cfg.DBNumber = dbNumber

	client, err = conn(cfg)
	return
}

func connByConnByte(connByte []byte) (client *redis.Client, err error) {
	cfg := new(dbConn)
	if err := json.Unmarshal(connByte, cfg); err != nil {
		return nil, err
	}

	client, err = conn(cfg)
	return
}

func conn(cfg *dbConn) (*redis.Client, error) {

	cli := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DBNumber,
	})

	_, err := cli.Ping().Result()
	if err != nil {
		return nil, err
	}

	return cli, nil
}
