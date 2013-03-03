package goqless

import (
  "fmt"
  "github.com/garyburd/redigo/redis"
  "sync"
  "testing"
  "time"
)

// func TestRandom(t *testing.T) {
//   c, err := Dial("", "6379")
//   if err != nil {
//     panic(err)
//   }
//   defer c.Close()

//   // fmt.Println(c.Completed(0, 9999))

//   tagged, err := c.Tagged("__callback", 0, 0)
//   if err != nil {
//     fmt.Println(err)
//   }

//   if err == nil {
//     job := NewJob(c)
//     for n, j := range tagged.Jobs {
//       fmt.Println(n, j)
//       job.Jid = j
//       fmt.Println(job.Untag("__callback"))
//     }
//   }
// }

func TestGoqless(t *testing.T) {
  var wg sync.WaitGroup

  c, err := Dial("", "6379")
  if err != nil {
    panic(err)
  }
  defer c.Close()

  e := c.Events()
  ch, err := e.Listen()
  if err != nil {
    panic(err)
  }

  go func() {
    wg.Add(1)
    defer wg.Done()

    for {
      if val, ok := <-ch; ok {
        switch v := (val).(type) {
        case redis.Message:
          fmt.Printf("WATCH: %s: message: %s\n", v.Channel, v.Data)
        case redis.Subscription:
          fmt.Printf("WATCH: %s: %s %d\n", v.Channel, v.Kind, v.Count)
        case error:
          return
        }
      } else {
        return
      }
    }
  }()

  jid := generateJID()
  c.Track(jid)
  q := c.Queue("goqless_testing_queue")

  putreply, err := q.Put(jid, "dunno", `{"hey": "there"}`, -1, -1, []string{"__callback"}, -1, []string{})
  fmt.Println("Put:", putreply, err)
  //fmt.Println(q.Recur(jid, "dunno", `{"hey": "there"}`, 5, 0, 0, []string{}, 1))

  for {
    jobs, err := q.Pop(1)
    if err != nil {
      panic(err)
    }

    if len(jobs) > 0 {
      jobs[0].Data = map[string]interface{}{"idid": "id"}
      fmt.Println(jobs[0].Complete())
    }
    time.Sleep(3 * time.Second)

    fmt.Println(c.GetJob(jid))
  }

  wg.Wait()
}
