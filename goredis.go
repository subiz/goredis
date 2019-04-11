package goredis

import (
	"strconv"
	"time"

	"errors"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
)

type Client struct {
	clients Map
	shard   int

	// ExpireDuration is total of time value being keep in redis
	ExpireDuration time.Duration
}

func (c *Client) connectTo(i int, redishost, password string) error {
	client := redis.NewClient(&redis.Options{Addr: redishost, Password: password})
	if _, err := client.Ping().Result(); err != nil {
		return errors.New("cannot_connect_to_redis")
	}

	c.clients.Set(strconv.Itoa(i), client)
	return nil
}

func (c *Client) GetKey(key string) string {
	return strconv.Itoa(int(fnv32(key)) % c.shard)
}

func New(hosts []string, password string) (*Client, error) {
	c := &Client{}
	c.ExpireDuration = 24 * time.Hour
	c.shard = len(hosts)

	c.clients = NewMap(len(hosts) * 2)
	reschan := make(chan error, len(hosts))
	for i, host := range hosts {
		go func(i int, host string) {
			reschan <- c.connectTo(i, host, password)
		}(i, host)
	}

	for range hosts {
		err := <-reschan
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Load data from redis cache
// return ErrNotFound if not found
func (c *Client) Load(key string, m proto.Message) (bool, error) {
	b, has, err := c.Get(key, key)
	if err != nil {
		return false, err
	}

	if !has {
		return false, err
	}

	if err := proto.Unmarshal(b, m); err != nil {
		return false, errors.New("goredis Load: proto marshal err: " + err.Error())
	}
	return true, nil
}

// Store val to redis cache and local cache
func (c *Client) Store(key string, val proto.Message) error {
	if b, err := proto.Marshal(val); err != nil {
		return errors.New("goredis Store: proto marshal err: " + err.Error())
	} else {
		return c.Set(key, key, b, c.ExpireDuration)
	}
}

func (c *Client) Get(shardkey, key string) ([]byte, bool, error) {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return nil, false, errors.New("goredis Get: redis client is uninitialized")
	}
	client := clienti.(*redis.Client)
	b, err := client.Get(key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.New("goredis Get: redis err " + err.Error())
	}
	return b, true, nil
}

func (c *Client) Set(shardkey, key string, value []byte, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New("goredis Set: redis client is uninitialized")
	}
	client := clienti.(*redis.Client)

	if err := client.Set(key, value, dur).Err(); err != nil {
		return errors.New("goredis Set (2): redis err " + err.Error())
	}
	return nil
}

func (c *Client) Expire(shardkey, key string, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New("goredis Expire: redis client is uninitialized")
	}
	client := clienti.(*redis.Client)
	if dur <= 0 {
		if _, err := client.Del(key).Result(); err != nil {
			return errors.New("goredis Expire (1): redis err " + err.Error())
		}
	}
	if _, err := client.Expire(key, dur).Result(); err != nil {
		return errors.New("goredis Expire (2): redis err " + err.Error())
	}
	return nil
}

func (c *Client) Incr(shardkey, key string, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New("goredis Incr: redis client is uninitialized")
	}
	client := clienti.(*redis.Client)
	if dur <= 0 {
		if _, err := client.Incr(key).Result(); err != nil {
			return errors.New("goredis Incr (1): redis err " + err.Error())
		}
	}
	pipe := client.TxPipeline()
	incr := pipe.Incr(key)
	pipe.Expire(key, dur)
	if _, err := pipe.Exec(); err != nil {
		return errors.New("goredis Incr (2): redis err " + err.Error())
	}
	incr.Val()
	return nil
}
