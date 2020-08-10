package etcd

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"os"
	"time"
)

var (
	CreateKvErr = errors.New("Failed to acquire lock")
)

func Lock(client *clientv3.Client, lockKey string, callBack func() error) (err error) {
	//mux.Lock()
	//defer mux.Unlock()
	//捕获异常
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	//创建KEY
	kv := clientv3.NewKV(client)
	txn := kv.Txn(context.TODO())
	//开始抢锁事务操作
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).Then(
		clientv3.OpPut(lockKey, ""),
	).Else(
		clientv3.OpGet(lockKey),
	)
	//提交事务
	txnRes, err := txn.Commit()
	if err != nil {
		return err
	}
	if txnRes.Succeeded { //抢锁成功
		defer func() {
			kv.Delete(context.TODO(), lockKey)
		}()
		return callBack()
	} else { //抢锁失败
		return CreateKvErr
	}
}

func LockTtl(client *clientv3.Client, lockKey string, ttl int64, callBack func() error) (err error) {
	//mux.Lock()
	//defer mux.Unlock()
	//捕获异常
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	//创建租约
	lease := clientv3.NewLease(client)
	leaseRes, err := lease.Grant(context.TODO(), ttl)
	if err != nil {
		return err
	}
	leaseID := leaseRes.ID
	//创建KEY
	kv := clientv3.NewKV(client)
	txn := kv.Txn(context.TODO())
	//开始抢锁事务操作
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).Then(
		clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID)),
	).Else(
		clientv3.OpGet(lockKey),
	)
	//提交事务
	txnRes, err := txn.Commit()
	if err != nil {
		return err
	}
	if txnRes.Succeeded { //抢锁成功
		return callBack()
	} else { //抢锁成功
		return CreateKvErr
	}
}

func LockKeepAlive(client *clientv3.Client, lockKey string, ttl int64, callBack func() error) (err error) {
	//mux.Lock()
	//defer mux.Unlock()
	//捕获异常
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	//创建租约
	lease := clientv3.NewLease(client)
	leaseRes, err := lease.Grant(context.TODO(), ttl)
	if err != nil {
		return err
	}
	leaseID := leaseRes.ID
	//创建KEY
	kv := clientv3.NewKV(client)
	txn := kv.Txn(context.TODO())
	//开始抢锁事务操作
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).Then(
		clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID)),
	).Else(
		clientv3.OpGet(lockKey),
	)
	//提交事务
	txnRes, err := txn.Commit()
	if err != nil {
		return err
	}
	if txnRes.Succeeded { //抢锁成功
		ctx, cancel := context.WithCancel(context.TODO())
		//抢锁和占用期间，需要不停的续租，续租方法返回一个只读的channel
		keepAlive, err := lease.KeepAlive(ctx, leaseID)
		if err != nil {
			return err
		}
		defer func() {
			//两个defer用于释放锁
			cancel()
			lease.Revoke(ctx, leaseID)
		}()
		//续租
		go func() {
			for {
				select {
				case leaseKeepAliveResponse := <-keepAlive:
					if leaseKeepAliveResponse == nil {
						fmt.Fprintf(os.Stderr, "lease fail \n")
						return
					} else {
						fmt.Fprintf(os.Stderr, "get leaseRes \n")
					}
				case <-ctx.Done():
					return
				}
				if ttl > 1 {
					time.Sleep(time.Second * time.Duration(ttl-1))
				}
			}
		}()
		return callBack()
	} else { //抢锁成功
		return CreateKvErr
	}
}
