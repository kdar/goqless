package goqless

import (
  "crypto/rand"
  "crypto/sha1"
  "encoding/json"
  "errors"
  "fmt"
  "github.com/garyburd/redigo/redis"
  "io/ioutil"
  "log"
  "os"
  "path/filepath"
  "strings"
  "sync"
  "time"
)

type Job struct {
  Expires      int64
  Dependents   interface{}
  Tracked      bool
  Tags         interface{}
  Jid          string
  Retries      int
  Data         interface{}
  Queue        string
  State        string
  Reminaing    int
  Failure      interface{}
  History      interface{}
  Dependencies interface{}
  Klass        string
  Priority     int
  Worker       string
}

// Generates a random sha1
func RandomSha1() (string, error) {
  hasher := sha1.New()
  uuid := make([]byte, 16)
  n, err := rand.Read(uuid)
  if err != nil {
    return "", err
  } else if n != len(uuid) {
    return "", errors.New("Could not generate random []byte")
  }

  hasher.Write(uuid)
  return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func do() {
  var wg sync.WaitGroup
  funcs := make(map[string]*redis.Script)

  c, err := redis.Dial("tcp", ":6379")
  if err != nil {
    log.Fatal(err)
  }
  defer c.Close()

  go watch(&wg)

  err = filepath.Walk("qless-core", func(path string, f os.FileInfo, err error) error {
    if strings.HasSuffix(f.Name(), ".lua") {
      src, err := ioutil.ReadFile(path)
      if err != nil {
        log.Fatal(err)
      }
      script := redis.NewScript(-1, string(src))
      script.Load(c)

      funcs[f.Name()[:len(f.Name())-4]] = script
      fmt.Println("Loaded: ", f.Name()[:len(f.Name())-4])
    }

    return nil
  })
  if err != nil {
    log.Fatal(err)
  }

  now := time.Now().UTC().Unix()

  jid, _ := RandomSha1()

  //Track(0) | Track(0, 'track', jid, now, tag, ...) | Track(0, 'untrack', jid, now)
  replyb, err := redis.Bool(funcs["track"].Do(c, 0, "track", jid, now, ""))
  if err != nil {
    log.Fatal(err)
  }

  fmt.Println(replyb)

  //Put(1, queue, jid, klass, data, now, delay, [priority, p], [tags, t], [retries, r], [depends, '[...]'])
  reply, err := funcs["put"].Do(c, 1, "medchecker.report", jid, "dunno", `{"hey": "there"}`, now, 0)
  if err != nil {
    log.Fatal(err)
  }

  fmt.Println(redis.String(reply, err))

  //Queues(0, now, [queue])
  reply, err = funcs["queues"].Do(c, 0, now)
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(redis.String(reply, err))

  for {
    //Pop(1, queue, worker, count, now)
    reply, err = funcs["pop"].Do(c, 1, "medchecker.report", "testworker", 1, now)
    if err != nil {
      log.Fatal(err)
    }

    values, err := redis.Values(reply, err)
    if err != nil {
      log.Fatal(err)
    }

    if len(values) == 0 {
      break
    }

    var jobs []Job
    for _, val := range values {
      var job Job
      bs, _ := redis.Bytes(val, err)
      err := json.Unmarshal(bs, &job)
      if err != nil {
        log.Fatal(err)
      }
      jobs = append(jobs, job)
    }

    fmt.Println(jobs)

    //Cancel(0, id)
    reply, err = funcs["cancel"].Do(c, 0, jobs[0].Jid)
    if err != nil {
      log.Fatal(err)
    }
  }

  wg.Wait()
}

func watch(wg *sync.WaitGroup) {
  wg.Add(1)
  defer wg.Done()
  var wg2 sync.WaitGroup

  c, err := redis.Dial("tcp", ":6379")
  if err != nil {
    log.Fatal(err)
  }
  defer c.Close()

  psc := redis.PubSubConn{c}

  go func() {
    wg2.Add(1)
    defer wg2.Done()
    for {
      switch v := psc.Receive().(type) {
      case redis.Message:
        fmt.Printf("WATCH: %s: message: %s\n", v.Channel, v.Data)
      case redis.Subscription:
        fmt.Printf("WATCH: %s: %s %d\n", v.Channel, v.Kind, v.Count)
      case error:
        log.Fatal("WATCH:", v)
      default:
        fmt.Println("WATCH:", v)
      }
    }
  }()

  for _, i := range []string{"canceled", "completed", "failed", "popped", "stalled", "put", "track", "untrack"} {
    psc.Subscribe(i)
  }

  wg2.Wait()
}
