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

func (c *Client) connectTo(redishost, password string) error {
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

func (c *Client) Connect(hosts []string, password string) error {
	c.locks = cmap.New(len(hosts) * 2) // decrease collition rate of key
	c.clients = cmap.New(len(hosts) * 2)
	reschan := make(chan error, len(hosts))
	for _, host := range hosts {
		go func(host string) {
			reschan <- c.connectTo(host, password)
		}(host)
	}

	for range hosts {
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

func (c *Client) Expire(shardkey, key string, dur time.Duration) error {
	clienti, ok := c.clients.Get(shardkey)
	if !ok {
		return errors.New("should not hapend, client is not init")
	}
	client := clienti.(*redis.Client)
	_, err := client.Expire(key, dur).Result()
	return err
}
