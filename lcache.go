package goredis

import (
	"sync"
	"time"

	common "bitbucket.org/subiz/gocommon"
	cmap "bitbucket.org/subiz/map"
	"github.com/golang/protobuf/proto"
)

// Lcache use lock key with concurrent map & store data cache into redis
type Lcache struct {
	rclient *Client
	lock    cmap.Map
}

type ILcache interface {
	Read(string, proto.Message) (proto.Message, *sync.Mutex, error)
	Save(string, proto.Message, time.Duration) error
	Delete(string) error
}

func NewLcache(len int, redishosts []string, redispassword string) ILcache {
	c := &Lcache{}
	c.lock = cmap.New(len)
	c.rclient = &Client{}
	err := c.rclient.Connect(redishosts, redispassword)
	if err != nil {
		common.LogErr(err)
	}
	return c
}

func (c *Lcache) Read(key string, data proto.Message) (proto.Message, *sync.Mutex, error) {
	mui, _ := c.lock.GetOrInit(key, func() interface{} {
		return &sync.Mutex{}
	})
	locker := mui.(*sync.Mutex)
	locker.Lock()
	defer locker.Unlock()

	rdata, err := c.rclient.Get(key, key)
	if err == nil {
		if err := proto.Unmarshal(rdata, data); err != nil {
			return data, locker, err
		}
		common.Log("cache hits", key)
		return data, locker, nil
	}

	common.Log("cache miss", key)
	return nil, locker, err
}

func (c *Lcache) Save(key string, data proto.Message, expire time.Duration) error {
	byts := common.Protify(data)
	return c.rclient.Set(key, key, byts, expire)
}

func (c *Lcache) Delete(key string) error {
	return c.rclient.Del(key, key)
}
