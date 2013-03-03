package goqless

import (
  "encoding/json"
  "fmt"
  "github.com/garyburd/redigo/redis"
)

type TaggedReply struct {
  Total int
  Jobs  StringSlice
}

type Client struct {
  conn redis.Conn
  host string
  port string

  events *Events
  lua    *Lua
}

func NewClient(host, port string) *Client {
  return &Client{host: host, port: port}
}

func Dial(host, port string) (*Client, error) {
  c := NewClient(host, port)

  conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
  if err != nil {
    return nil, err
  }

  c.lua = NewLua(conn)
  // _, filename, _, _ := runtime.Caller(0)
  err = c.lua.LoadScripts("qless-core") // make get from lib path
  if err != nil {
    conn.Close()
    return nil, err
  }

  c.conn = conn
  return c, nil
}

func (c *Client) Close() {
  c.conn.Close()
}

func (c *Client) Events() *Events {
  if c.events != nil {
    return c.events
  }
  c.events = NewEvents(c.host, c.port)
  return c.events
}

func (c *Client) Do(name string, keysAndArgs ...interface{}) (interface{}, error) {
  return c.lua.Do(name, keysAndArgs...)
}

func (c *Client) Queue(name string) *Queue {
  q := NewQueue(c)
  q.Name = name
  return q
}

// Queues(0, now, [queue])
func (c *Client) Queues(name string) ([]*Queue, error) {
  args := []interface{}{0, timestamp()}
  if name != "" {
    args = append(args, name)
  }

  byts, err := redis.Bytes(c.Do("queues", args...))
  if err != nil {
    return nil, err
  }

  qr := []*Queue{NewQueue(c)}
  if name == "" {
    err = json.Unmarshal(byts, &qr)
    for _, q := range qr {
      q.cli = c
    }
  } else {
    err = json.Unmarshal(byts, &qr[0])
  }

  if err != nil {
    return nil, err
  }

  return qr, err
}

// Track(0, 'track', jid, now, tag, ...)
// Track the jid
func (c *Client) Track(jid string) (bool, error) {
  return Bool(c.Do("track", 0, "track", jid, timestamp(), ""))
}

// Track(0, 'untrack', jid, now)
// Untrack the jid
func (c *Client) Untrack(jid string) (bool, error) {
  return Bool(c.Do("track", 0, "untrack", jid, timestamp()))
}

// Track(0)
// Returns all the tracked jobs
func (c *Client) Tracked() (string, error) {
  return redis.String(c.Do("track", 0))
}

func (c *Client) Get(jid string) (interface{}, error) {
  job, err := c.GetJob(jid)
  if err == redis.ErrNil {
    rjob, err := c.GetRecurringJob(jid)
    return rjob, err
  }
  return job, err
}

func (c *Client) GetJob(jid string) (*Job, error) {
  byts, err := redis.Bytes(c.Do("get", 0, jid))
  if err != nil {
    return nil, err
  }

  job := NewJob(c)
  err = json.Unmarshal(byts, job)
  if err != nil {
    return nil, err
  }
  return job, err
}

func (c *Client) GetRecurringJob(jid string) (*RecurringJob, error) {
  byts, err := redis.Bytes(c.Do("recur", 0, "get", jid))
  if err != nil {
    return nil, err
  }

  job := NewRecurringJob(c)
  err = json.Unmarshal(byts, job)
  if err != nil {
    return nil, err
  }
  return job, err
}

func (c *Client) Completed(start, count int) ([]string, error) {
  reply, err := redis.Values(c.Do("jobs", 0, "complete"))
  if err != nil {
    return nil, err
  }

  // fmt.Println(reply)

  ret := []string{}
  for _, val := range reply {
    s, _ := redis.String(val, err)
    ret = append(ret, s)
  }
  return ret, err
}

func (c *Client) Tagged(tag string, start, count int) (*TaggedReply, error) {
  byts, err := redis.Bytes(c.Do("tag", 0, "get", tag, start, count))
  if err != nil {
    return nil, err
  }

  t := &TaggedReply{}
  err = json.Unmarshal(byts, t)
  return t, err
}

// // returns all the failed jobs
// func (c *Client) Failed(group string, start, limit int) ([]*Job, error) {
//   c.Do("failed", 0,
// }

// config(0, 'get', [option])
func (c *Client) GetConfig(option string) string {
  args := []interface{}{0}
  if option != "" {
    args = append(args, option)
  }

  str, _ := redis.String(c.Do("get", args...))
  return str
}

// config(0, 'set', option, value)
func (c *Client) SetConfig(option string, value interface{}) {
  c.Do("set", option, value)
}

// config(0, 'unset', [option])
func (c *Client) UnsetConfig(option string) {
  c.Do("unset", option)
}
