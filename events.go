package goqless

import (
  "fmt"
  "github.com/garyburd/redigo/redis"
)

type Events struct {
  conn redis.Conn
  ch   chan interface{}
  psc  *redis.PubSubConn
  host string
  port string
}

func NewEvents(host, port string) *Events {
  return &Events{host: host, port: port}
}

func (e *Events) Listen() (chan interface{}, error) {
  var err error
  e.conn, err = redis.Dial("tcp", fmt.Sprintf("%s:%s", e.host, e.port))
  if err != nil {
    return nil, err
  }

  e.psc = &redis.PubSubConn{e.conn}
  e.ch = make(chan interface{}, 1)

  go func() {
    for {
      val := e.psc.Receive()
      if v, ok := val.(error); ok {
        e.ch <- v
        close(e.ch)
        break
      }
      e.ch <- val
    }
  }()

  for _, i := range []string{"canceled", "completed", "failed", "popped", "stalled", "put", "track", "untrack"} {
    err := e.psc.Subscribe(i)
    if err != nil {
      close(e.ch)
      return nil, err
    }
  }

  return e.ch, nil
}

func (e *Events) Unsubscribe() {
  if e.psc != nil {
    e.psc.Unsubscribe()
    e.psc.Close()
  }
}
