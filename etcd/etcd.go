package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"os"
	"strings"
	"time"
)

type etcd struct {
	endpoints []string
	userName  string
	passWord  string
	cli       *clientv3.Client
	err       error
}

func Conn(endpoints ...string) *etcd {
	etcd := new(etcd)
	etcd.endpoints = endpoints
	return etcd
}

func (e *etcd) Auth(un, up string) *etcd {
	e.userName = un
	e.passWord = up
	return e
}

/*
获取ETCD地址列表
格式：ETCD_ADDR=192.168.1.1:1000,192.168.1.1:1000,192.168.1.1:1000
*/
func ConnByEnv(env string) *etcd {
	etcd := new(etcd)
	addr, ok := os.LookupEnv(env)
	if !ok {
		etcd.err = fmt.Errorf("%w", errors.New("ETCD_ADDR not found"))
		return etcd
	}
	etcd.endpoints = strings.Split(strings.TrimSpace(addr), ",")
	return etcd
}

/**
获取ETCD客户端
*/
func (e *etcd) etcdClient() *clientv3.Client {
	if len(e.endpoints) == 0 || e.endpoints[0] == "" {
		e.err = fmt.Errorf("%w", errors.New("ETCD_ADDR not found"))
		return nil
	}
	//获取客户端对象
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        e.endpoints,
		AutoSyncInterval: time.Hour,
		DialTimeout:      time.Second * 10,
		Username:         e.userName,
		Password:         e.passWord,
	})
	if err != nil {
		e.err = fmt.Errorf("%w", err)
		return nil
	}

	return cli
}

/**
获取ETCD客户端
*/
func GetClient(endpoints ...string) *clientv3.Client {
	return Conn(endpoints...).etcdClient()
}

/**
获取ETCD客户端
*/
func GetClientByEnv(env string) *clientv3.Client {
	return ConnByEnv(env).etcdClient()
}

func (e *etcd) Get(key string) ([]byte, error) {
	cli := e.etcdClient()
	if e.err != nil {
		return nil, e.err
	}
	ctx, cencel := context.WithTimeout(context.Background(), time.Second*5)
	defer cencel()

	res, err := cli.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(res.Kvs) == 0 {
		return nil, errors.New("key 对应的值为空")
	}

	fmt.Println(string(res.Kvs[0].Value))
	return res.Kvs[0].Value, nil
}
