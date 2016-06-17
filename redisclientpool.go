package gredis

import(
	"github.com/yuwnloyblog/commonspool"
)

type RedisClientPool struct{
	pool *commonspool.ObjectPool
	Host string
	Port int
}

func NewRedisPoolDefault(host string,port int)*RedisClientPool{
	return NewRedisPool(host,port,nil)
}

func NewRedisPool(host string, port int, config *commonspool.ObjectPoolConfig)*RedisClientPool{
	pool := &RedisClientPool{
		Host:host,
		Port:port,
	}
	err:=pool.init(config)
	if err!= nil{
		return nil
	}
	return pool
}

/**
 * initial the redis pool
 */
func (self *RedisClientPool) init(config *commonspool.ObjectPoolConfig)error{
	var objconfig *commonspool.ObjectPoolConfig
	if config != nil {
		objconfig = config
	}else{
		objconfig = commonspool.NewDefaultPoolConfig()
	}
	var poolfactory = NewRedisFactory(self.Host,self.Port)
	self.pool = commonspool.NewObjectPool(poolfactory,objconfig)
	return nil
}

/**
 * get the object from pool
 */
func (self *RedisClientPool) GetResource()(*RedisClient,error){
	pooledObj,err:=self.pool.BorrowObject()
	if pooledObj != nil{
		return pooledObj.(*RedisClient),err
	}
	return nil,&RedisError{"Error when borrow redis client from pool."}
}

/**
 * destroy the redis pool
 */
func (self *RedisClientPool) Destroy()error{
	if self.pool == nil{
		return &RedisError{"The pool have no initial."}
	}
	self.pool.Close()
	return nil
}

/**
 * return redis client to pool
 */
func (self *RedisClientPool) ReturnResource(rc interface{})error{
	if self.pool == nil{
		return &RedisError{"The pool have no initial."}
	}
	return self.pool.ReturnObject(rc)
}