package providers

import (
	"errors"
	"github.com/go-redis/redis"
	"fmt"
)

type RedisProvider struct {
	StorageProvider

	redisClient *redis.Client

	host		string
	password 	string
	db_num		int
}

func NewRedisProvider(host string, password string, db_num int) *RedisProvider {
	return &RedisProvider{host: host, password: password, db_num: db_num}
}

func (rp *RedisProvider) Initialize(actionName string) {
	//dbNum, err := strconv.Atoi(os.Getenv("REDIS_STORAGE_DB_NUM"))
	//if err != nil {
	//	dbNum = 0
	//}

	rp.redisClient = redis.NewClient(&redis.Options{
		Addr:     rp.host,//os.Getenv("REDIS_STORAGE_ADDR"),
		Password: rp.password,//os.Getenv("REDIS_STORAGE_PASSWORD"),
		DB:       rp.db_num,//dbNum,
	})

	pong, err := rp.redisClient.Ping().Result()
	fmt.Println(pong, err)
}
func (rp *RedisProvider) Close() {
	if rp.redisClient != nil {
		rp.redisClient.Close()
	}
}

func (rp *RedisProvider) Increment(key string) (int, error) {
	if rp.redisClient == nil {
		return 0, errors.New("Storage Provider is not initialized")
	}

	val, err := rp.redisClient.Incr(key).Result()

	return int(val), err
}

func (rp *RedisProvider) Set(key string, value string) error {
	if rp.redisClient == nil {
		return errors.New("Storage Provider is not initialized")
	}

	err := rp.redisClient.Set(key, value, 0).Err()

	return err
}

func (rp *RedisProvider) Get(key string) (string, error) {
	if rp.redisClient == nil {
		return "", errors.New("Storage Provider is not initialized")
	}

	val, err := rp.redisClient.Get(key).Result()

	return val, err
}
