package etcd

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"sync"
)

var (
	mux         sync.Mutex
	CreateKvErr = errors.New("Failed to acquire lock")
	//lease       clientv3.Lease
	//leaseID     clientv3.LeaseID
)

const lockKey = "/cmany_db/lock"

func Lock(client *clientv3.Client, ttl int64, callBack func() error) (err error) {
	mux.Lock()
	defer mux.Unlock()
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
			//两个defer用于释放锁
			kv.Delete(context.TODO(), lockKey)
		}()

		err = callBack()
		if err != nil {
			return err
		}
		return nil
	} else { //抢锁成功
		return CreateKvErr
	}
}

//func Lock2(client *clientv3.Client, ttl int64, callBack func() error) (err error) {
//	mux.Lock()
//	defer mux.Unlock()
//	//捕获异常
//	defer func() {
//		if e := recover(); e != nil {
//			err = fmt.Errorf("%v", e)
//		}
//	}()
//	//创建租约
//	if leaseID == 0 {
//		lease = clientv3.NewLease(client)
//		leaseRes, err := lease.Grant(context.TODO(), ttl)
//		if err != nil {
//			return err
//		}
//		leaseID = leaseRes.ID
//	}
//	//创建KEY
//	kv := clientv3.NewKV(client)
//	txn := kv.Txn(context.TODO())
//	//开始抢锁事务操作
//	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).Then(
//		clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID)),
//	).Else(
//		clientv3.OpGet(lockKey),
//	)
//	//提交事务
//	txnRes, err := txn.Commit()
//	if err != nil {
//		return err
//	}
//	if txnRes.Succeeded { //抢锁成功
//		ctx, cancel := context.WithCancel(context.TODO())
//		//抢锁和占用期间，需要不停的续租，续租方法返回一个只读的channel
//		keepAlive, err := lease.KeepAlive(ctx, leaseID)
//		if err != nil {
//			return err
//		}
//		defer func() {
//			//两个defer用于释放锁
//			cancel()
//			lease.Revoke(ctx, leaseID)
//		}()
//		//续租
//		go func() {
//			for {
//				select {
//				case leaseKeepAliveResponse := <-keepAlive:
//					if leaseKeepAliveResponse == nil {
//						fmt.Println("lease fail!")
//						goto END
//					} else {
//						fmt.Println("get leaseRes", leaseKeepAliveResponse.ID)
//					}
//				}
//			}
//		END:
//		}()
//
//		err = callBack()
//		if err != nil {
//			return err
//		}
//		return nil
//	} else { //抢锁成功
//		return CreateKvErr
//	}
//}
