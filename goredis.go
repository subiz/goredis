package goredis

import (
	"strconv"
	"time"

	"git.subiz.net/errors"
	"git.subiz.net/goutils/map"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
)

type Client struct {
	// locks   cmap.Map
	clients cmap.Map
	shard   int

	// ExpireDuration is total of time value being keep in redis
	ExpireDuration time.Duration
}

func (c *Client) connectTo(i int, redishost, password string) error {
	client := redis.NewClient(&redis.Options{Addr: redishost, Password: password})
	if _, err := client.Ping().Result(); err != nil {
		return errors.Wrap(err, 500, errors.E_cannot_connect_to_redis, redishost)
	}
	// c.locks.Set(redishost, client)
	c.clients.Set(strconv.Itoa(i), client)
	return nil
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (c *Client) GetKey(key string) string {
	return strconv.Itoa(int(fnv32(key)) % c.shard)
}

func New(hosts []string, password string) (*Client, error) {
	c := &Client{}
	c.ExpireDuration = 24 * time.Hour
	c.shard = len(hosts)
	// c.locks = cmap.New(len(hosts) * 2)
	c.clients = cmap.New(len(hosts) * 2)
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
func (c *Client) Load(key string, m proto.Message) error {
	b, err := c.Get(key, key)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b, m); err != nil {
		return errors.Wrap(err, 500, errors.E_proto_marshal_error)
	}
	return nil
}

// Store val to redis cache and local cache
func (c *Client) Store(key string, val proto.Message) error {
	if b, err := proto.Marshal(val); err != nil {
		return errors.Wrap(err, 400, errors.E_proto_marshal_error)
	} else {
		return c.Set(key, key, b, c.ExpireDuration)
	}
}

func (c *Client) Get(shardkey, key string) ([]byte, error) {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return nil, errors.New(500, errors.E_redis_client_is_uninitialized)
	}
	client := clienti.(*redis.Client)
	str, err := client.Get(key).Bytes()
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_redis_error)
	}
	return str, nil
}

func (c *Client) Set(shardkey, key string, value []byte, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New(500, errors.E_redis_client_is_uninitialized)
	}
	client := clienti.(*redis.Client)

	if err := client.Set(key, value, dur).Err(); err != nil {
		return errors.Wrap(err, 500, errors.E_redis_error)
	}
	return nil
}

func (c *Client) Expire(shardkey, key string, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New(500, errors.E_redis_client_is_uninitialized)
	}
	client := clienti.(*redis.Client)
	if dur <= 0 {
		if _, err := client.Del(key).Result(); err != nil {
			return errors.Wrap(err, 500, errors.E_redis_error)
		}
	}
	if _, err := client.Expire(key, dur).Result(); err != nil {
		return errors.Wrap(err, 500, errors.E_redis_error)
	}
	return nil
}
