package gredis
import(
	"github.com/yuwnloyblog/go-commons-tool/utils"
	"strconv"
	"fmt"
)

type ShardedRedis struct{
	consisHash *utils.ConsistentHash
	
}
/**
 * create sharded redis client
 */
func NewShardedRedis()*ShardedRedis{
	return &ShardedRedis{
		consisHash : utils.NewConsistentHash(),
	}
}

func (self *ShardedRedis)Add(name,host string,port,weight int)bool{
	if name == ""{
		name = host + strconv.Itoa(port)
	}
	pool := NewRedisPoolDefault(host,port)
	return self.consisHash.Add(name, pool, weight)
}
/**
 * get the redispool by key base consistent hash
 */
func (self *ShardedRedis)getRedisPool(key string)*RedisClientPool{
	node:=self.consisHash.Get(key)
	if node != nil{
		return node.Entry.(*RedisClientPool)
	}
	return nil	
}
/**
 * 1. get the redis pool
 * 2. get the redis conn
 * 3. execute the cmd
 * 4. release conn to pool
 */
func (self *ShardedRedis)handleMethod(cmd string,args ...interface{}) (interface{}, error){
	if args != nil{
		key := args[0].(string)
		pool := self.getRedisPool(key)
		fmt.Printf("k:%s, name:%s\n",key,pool.Host)
		if pool != nil{
			client,err:= pool.GetResource()
			if err != nil{
				return nil,err
			}else{
				defer pool.ReturnResource(client)//release the redis client to pool
				return client.Do(cmd,args...)
			}
		}
	}
	return nil,&RedisError{"Sharded redis error."}
}

func (self *ShardedRedis)Do(cmd string,args ...interface{})(interface{},error){
	return self.handleMethod(cmd,args...)
}

//Get key
func (self *ShardedRedis) Get(key string)([]uint8,error){
	ret,err := self.handleMethod("GET",key)
	if ret != nil{
		return ret.([]uint8),err
	}
	return nil , err
}

//Set key value
func (self *ShardedRedis) Set(key string, value interface{})(string,error){
	ret,err := self.handleMethod("SET", key, value)
	if ret != nil{
		return ret.(string),err
	}
	return "FAIL", err
}