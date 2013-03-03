// This worker does not model the way qless does it.
// I more or less modeled it after my own needs.
package goqless

import (
  // "encoding/json"
  "fmt"
  // "github.com/garyburd/redigo/redis"

  "reflect"
  "strconv"
  "time"
)

type JobFunc func(*Job) error
type JobCallback func(*Job) error

type Worker struct {
  Interval int // in time.Duration

  funcs  map[string]JobFunc
  queues []*Queue
  // events *Events

  cli *Client
}

func NewWorker(cli *Client, queues []string, interval int) {
  w := &Worker{
    Interval: interval,
    funcs:    make(map[string]JobFunc),
    // events:   c.Events(),
    cli: cli,
  }

  for _, q := range queues {
    w.queues = append(w.queues, cli.Queue(q))
  }
}

func (w *Worker) Start() error {
  heartbeatc := w.cli.GetConfig("heartbeat")
  heartbeat, err := strconv.ParseInt(heartbeatc, 10, 64)
  if err != nil {
    heartbeat = 60
  }

  // var wg sync.WaitGroup
  // ch, err := w.events.Listen()
  // if err != nil {
  //   return err
  // }

  // go func() {
  //   wg.Add(1)
  //   defer wg.Done()

  //   for {
  //     if val, ok := <-ch; ok {
  //       switch v := (val).(type) {
  //       case redis.Message:
  //         // v.Channel, v.Data

  //       case redis.Subscription:
  //         // fmt.Printf("WATCH: %s: %s %d\n", v.Channel, v.Kind, v.Count)
  //       case error:
  //         return
  //       }
  //     } else {
  //       return
  //     }
  //   }
  // }()

  for {
    for _, q := range w.queues {
      jobs, err := q.Pop(1)
      if err != nil {
        // report to some error channel?
      } else {
        go func() {
          // TODO: heartbeating. should i make workers do this?
          exitHeartbeat := false
          go func() {
            time.Sleep(time.Duration(heartbeat) * time.Second)
            if exitHeartbeat {
              return
            }
            jobs[0].Heartbeat()
          }()

          err := w.funcs[jobs[0].Jid](jobs[0])
          if err != nil {
            // TODO: probably do something with this
            jobs[0].Fail("fail", err.Error())
          } else {
            jobs[0].Complete()
          }

          exitHeartbeat = true
        }()
      }
    }

    time.Sleep(time.Duration(w.Interval))
  }

  // wg.Wait()

  return nil
}

func (w *Worker) AddFunc(name string, f JobFunc) error {
  if _, ok := w.funcs[name]; ok {
    return fmt.Errorf("function \"%s\" already exists", name)
  }

  w.funcs[name] = f
  return nil
}

// Adds all the methods in the passed interface as job functions.
// Job names are in the form of: name.methodname
func (w *Worker) AddService(name string, rcvr interface{}) error {
  typ := reflect.TypeOf(rcvr)
  val := reflect.ValueOf(rcvr)
  for i := 0; i < typ.NumMethod(); i++ {
    method := typ.Method(i)
    w.AddFunc(name+"."+method.Name, func(job *Job) error {
      ret := method.Func.Call([]reflect.Value{val, reflect.ValueOf(job)})
      if err, ok := ret[1].Interface().(error); ok {
        return err
      }

      return nil
    })
  }

  return nil
}
