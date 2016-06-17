package gredis
import(
	"fmt"
	"time"
)

type RedisError struct{
	What string
}
func (self *RedisError) Error() string {
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}