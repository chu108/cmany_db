package etcd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
)

type EtcdLock struct {
	client    *clientv3.Client
	ttl       int64
	lease     clientv3.Lease
	leaseID   clientv3.LeaseID
	txn       clientv3.Txn
	cancel    context.CancelFunc
	ctx       context.Context
	keepAlive <-chan *clientv3.LeaseKeepAliveResponse
	lockKey   string
}

//初始化锁
func NewEtcdLock(client *clientv3.Client, ttl int64) (*EtcdLock, error) {
	el := new(EtcdLock)
	el.client = client
	el.ttl = ttl
	el.lockKey = "/cmany_db/lock"
	//创建租约
	el.lease = clientv3.NewLease(client)
	leaseRes, err := el.lease.Grant(context.TODO(), ttl)
	if err != nil {
		return nil, err
	}
	el.leaseID = leaseRes.ID
	el.ctx, el.cancel = context.WithCancel(context.TODO())
	//抢锁和占用期间，需要不停的续租，续租方法返回一个只读的channel
	el.keepAlive, err = el.lease.KeepAlive(el.ctx, el.leaseID)
	if err != nil {
		return nil, err
	}
	return el, nil
}

//获取锁
func (el *EtcdLock) Lock() (bool, error) {
	//创建KEY
	kv := clientv3.NewKV(el.client)
	el.txn = kv.Txn(context.TODO())
	//开始抢锁事务操作
	el.txn.If(clientv3.Compare(clientv3.CreateRevision(el.lockKey), "=", 0)).Then(
		clientv3.OpPut(el.lockKey, "", clientv3.WithLease(el.leaseID)),
	).Else(
		clientv3.OpGet(el.lockKey),
	)
	//提交事务
	txnRes, err := el.txn.Commit()
	if err != nil {
		return false, err
	}
	if txnRes.Succeeded { //抢锁成功
		//续租
		go func() {
			for {
				select {
				case leaseKeepAliveResponse := <-el.keepAlive:
					if leaseKeepAliveResponse == nil {
						fmt.Println("lease fail!")
						goto END
					} else {
						fmt.Println("get leaseRes", leaseKeepAliveResponse.ID)
					}
				}
			}
		END:
		}()

		return true, nil
	} else { //抢锁成功
		return false, nil
	}
}

//释放锁
func (el *EtcdLock) UnLock() {
	//两个defer用于释放锁
	el.cancel()
	el.lease.Revoke(el.ctx, el.leaseID)
}
