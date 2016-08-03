package gredis

import (
	"github.com/garyburd/redigo/redis"
	"strings"
	"net"
	"strconv"
)

type RedisClient struct{
	Host string
	Port int
	Conn redis.Conn
}

//connect 
func (self *RedisClient) connect()(error){
	server := strings.Join([]string{self.Host,strconv.Itoa(self.Port)},":")
	conn, err := redis.Dial("tcp", server)
	self.Conn = conn
    return err
}
//disconnect
func (self *RedisClient) Disconnect()(error){
	if self.Conn!=nil{
		err := self.Conn.Close()
		self.Conn = nil
		return err
	}
	return nil
}
// redis method
func (self *RedisClient)handleMethod(cmd string,args ...interface{}) (interface{}, error){
	if self.Conn == nil{
		err := self.connect()
		if err != nil{
			return nil,err
		}
	}
	ret,err := self.Conn.Do(cmd,args...)
	switch err.(type) {
		case *net.OpError:
			self.Conn = nil
	}
	return ret,err
}
func (self *RedisClient)Do(cmd string,args ...interface{})(interface{},error){
	return self.handleMethod(cmd,args...)
}
//Ping
func (self *RedisClient) Ping()(string,error){
	ret,err := self.handleMethod("PING")
	if ret != nil  && err==nil{
		return ret.(string),err
	}
	return "FAIL", err
}

//Get key
func (self *RedisClient) Get(key string)([]uint8,error){
	ret,err := self.handleMethod("GET",key)
	if ret != nil && err==nil{
		return ret.([]uint8),err
	}
	return nil , err
}
//Set key value
func (self *RedisClient) Set(key string, value interface{})(string,error){
	ret,err := self.handleMethod("SET", key, value)
	if ret != nil && err==nil{
		return ret.(string),err
	}
	return "FAIL", err
}
//Quit
func (self *RedisClient) Quit()(string,error){
	ret,err := self.handleMethod("QUIT")
	if ret != nil && err==nil{
		return ret.(string),err
	}
	return "FAIL", err
}
//Exists key
func (self *RedisClient) Exists(key string)(bool,error){
	ret,err := self.handleMethod("EXISTS", key)
	if ret != nil && err==nil{
		return ret.(int64)>0,err
	}
	return false,err
}

//mget
func (self *RedisClient)MGet(keys ...interface{})([][]uint8,error){
	ret,err := self.handleMethod("MGET",keys...)
	if ret != nil&&err==nil{
		var arr [][]uint8
		for _,v := range ret.([]interface{}){
			arr = append(arr, v.([]uint8))
		}
		return arr,nil
	}
	return nil,err
}
//mset
func (self *RedisClient)MSet(keyVals ...interface{})(string,error){
	ret,err := self.handleMethod("MSET", keyVals...)
	if err==nil&&ret!=nil{
		return ret.(string),err
	}
	return "",err
}