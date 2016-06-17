package gredis

import(
	"github.com/yuwnloyblog/commonspool"
)

type RedisClientPoolFactory struct{
	Host string
	Port int
}

func NewRedisFactory(host string, port int) commonspool.PooledObjectFactory {
	return &RedisClientPoolFactory{host,port}
}

func (self *RedisClientPoolFactory) MakeObject() (*commonspool.PooledObject, error) {
	var client = RedisClient{Host:self.Host,Port:self.Port}
	pooledObject := commonspool.NewPooledObject(&client)
	if self.ValidateObject(pooledObject) {
		return pooledObject,nil
	}
    return nil, &RedisError{"Create a invalidate object."}
}

func (self *RedisClientPoolFactory) DestroyObject(object *commonspool.PooledObject) error {
    if object != nil{
    	if object.Object!=nil{
    		client:=object.Object.(RedisClient)
    		return client.Disconnect()
    	}
    }
    return &RedisError{"Invalidate object."}
}

func (self *RedisClientPoolFactory) ValidateObject(object *commonspool.PooledObject) bool {
    if object != nil{
    	target := object.Object
    	if target != nil{
    		client:=target.(*RedisClient)
    		//client.Quit()
    		pret,err:=client.Ping()
    		if pret!="FAIL"&&err==nil{
    			if string(pret)=="PONG"{
    				return true
    			}
    		}
    	}
    }
    return false
}

func (self *RedisClientPoolFactory) ActivateObject(object *commonspool.PooledObject) error {
    //do activate
    return nil
}

func (f *RedisClientPoolFactory) PassivateObject(object *commonspool.PooledObject) error {
    //do passivate
    return nil
}