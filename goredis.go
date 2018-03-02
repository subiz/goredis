package goredis

import (
	"github.com/go-redis/redis"
	"bitbucket.org/subiz/map"
	"fmt"
	"errors"
	"time"
)

type ShardRedisClient Client

type Client struct {
	locks cmap.Map
	clients cmap.Map
}

func (c *Client) connectTo(prefix, password string, port, index int) error {
	redishost := fmt.Sprintf("%s%d:%d", prefix, index, port)
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
	return nil
}

func (c *Client) Connect(prefix, password string, port, shard int) error {
	c.locks = cmap.New(shard * 2) // decrease collition rate of key

	reschan := make(chan error, shard)
	for i := 0; i < shard; i++ {
		go func(i int) {
			reschan <- c.connectTo(prefix, password, port, i)
		}(i)
	}

	for i := 0; i < shard; i++ {
		err := <- reschan
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Get(shardkey, key string) ([]byte, error) {
	clienti, ok := c.clients.Get(shardkey)
	if !ok {
		return nil, errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	str, err := client.Get(key).Result()
	return []byte(str), err
}

func (c *Client) Set(shardkey, key string, value []byte, dur time.Duration) error {
	clienti, ok := c.clients.Get(shardkey)
	if !ok {
		return errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	return client.Set(key, value, dur).Err()
}
