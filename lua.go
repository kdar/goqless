package goqless

import (
  "fmt"
  "github.com/garyburd/redigo/redis"
  "io/ioutil"
  // "log"
  "os"
  "path/filepath"
  "strings"
)

type Lua struct {
  conn redis.Conn
  f    map[string]*redis.Script
}

func NewLua(c redis.Conn) *Lua {
  l := &Lua{c, make(map[string]*redis.Script)}
  return l
}

// loads all the given scripts from the path
func (l *Lua) LoadScripts(path string) error {
  err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
    if strings.HasSuffix(info.Name(), ".lua") {
      src, err := ioutil.ReadFile(path)
      if err != nil {
        return err
      }
      script := redis.NewScript(-1, string(src))
      err = script.Load(l.conn)
      if err != nil {
        return err
      }

      l.f[info.Name()[:len(info.Name())-4]] = script

      // log.Println("Loaded: ", info.Name()[:len(info.Name())-4])
    }

    return nil
  })

  return err
}

// calls a script with the arguments
func (l *Lua) Do(name string, keysAndArgs ...interface{}) (interface{}, error) {
  if fn, ok := l.f[name]; ok {
    return fn.Do(l.conn, keysAndArgs...)
  }

  return nil, fmt.Errorf("no script named %s found", name)
}
