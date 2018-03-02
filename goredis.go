package goredis

import (
	"bitbucket.org/subiz/map"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

type ShardRedisClient Client

type Client struct {
	locks   cmap.Map
	clients cmap.Map
	shard   int
}

func (c *Client) connectTo(i int, redishost, password string) error {
	client := redis.NewClient(&redis.Options{
		Addr:     redishost,
		Password: password,
	})
	pong, err := client.Ping().Result()
	if err != nil {
		fmt.Println("cannot connect to " + redishost + ": " + err.Error())
		return err
	}
	fmt.Println("pong from " + redishost + ": " + pong)
	c.locks.Set(redishost, client)
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

func (c *Client) Connect(hosts []string, password string) error {
	c.shard = len(hosts)
	c.locks = cmap.New(len(hosts) * 2) // decrease collition rate of key
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
			return err
		}
	}
	return nil
}

func (c *Client) Get(shardkey, key string) ([]byte, error) {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return nil, errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	str, err := client.Get(key).Result()
	return []byte(str), err
}

func (c *Client) Set(shardkey, key string, value []byte, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	return client.Set(key, value, dur).Err()
}

func (c *Client) Expire(shardkey, key string, dur time.Duration) error {
	clienti, ok := c.clients.Get(c.GetKey(shardkey))
	if !ok {
		return errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	_, err := client.Expire(key, dur).Result()
	return err
}
