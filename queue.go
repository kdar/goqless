package goqless

import (
  "encoding/json"
  "fmt"
  "github.com/garyburd/redigo/redis"
)

var _ = fmt.Sprint("")

type Queue struct {
  Running   int
  Name      string
  Waiting   int
  Recurring int
  Depends   int
  Stalled   int
  Scheduled int

  cli *Client
}

func NewQueue(cli *Client) *Queue {
  return &Queue{cli: cli}
}

func (q *Queue) SetClient(cli *Client) {
  q.cli = cli
}

// Jobs(0, ('stalled' | 'running' | 'scheduled' | 'depends' | 'recurring'), now, queue, [offset, [count]])
func (q *Queue) Jobs(state string, start, count int) ([]string, error) {
  reply, err := redis.Values(q.cli.Do("jobs", 0, state, timestamp(), q.Name))
  if err != nil {
    return nil, err
  }

  ret := []string{}
  for _, val := range reply {
    s, _ := redis.String(val, err)
    ret = append(ret, s)
  }
  return ret, err
}

// Cancel all jobs in this queue
func (q *Queue) CancelAll() {
  for _, state := range JOBSTATES {
    var jids []string
    for {
      jids, _ = q.Jobs(state, 0, 100)
      for _, jid := range jids {
        j, err := q.cli.GetRecurringJob(jid)
        if j != nil && err == nil {
          j.Cancel()
        }
      }

      if len(jids) < 100 {
        break
      }
    }
  }
}

// Pause(0, name)
func (q *Queue) Pause() {
  q.cli.Do("pause", 0, q.Name)
}

// Unpause(0, name)
func (q *Queue) Unpause() {
  q.cli.Do("unpause", 0, q.Name)
}

// Put(1, queue, jid, klass, data, now, delay, [priority, p], [tags, t], [retries, r], [depends, '[...]'])
// Puts a job into the queue
// returns jid, error
func (q *Queue) Put(jid, klass string, data interface{}, delay, priority int, tags []string, retries int, depends []string) (string, error) {
  if jid == "" {
    jid = generateJID()
  }
  if delay == -1 {
    delay = 0
  }
  if priority == -1 {
    priority = 0
  }
  if retries == -1 {
    retries = 5
  }

  return redis.String(q.cli.Do(
    "put", 1, q.Name, jid, klass,
    marshal(data), timestamp(),
    delay, "priority", priority,
    "tags", marshal(tags), "retries",
    retries, "depends", marshal(depends)))
}

// Pop(1, queue, worker, count, now)
// Pops a job off the queue.
func (q *Queue) Pop(count int) ([]*Job, error) {
  if count == 0 {
    count = 1
  }

  reply, err := redis.Values(q.cli.Do("pop", count, q.Name, workerName(), 1, timestamp()))
  if err != nil {
    return nil, err
  }

  var jobs []*Job
  for _, val := range reply {
    job := NewJob(q.cli)
    bs, _ := redis.Bytes(val, err)
    err := json.Unmarshal(bs, job)
    if err != nil {
      return nil, err
    }
    jobs = append(jobs, job)
  }

  return jobs, nil
}

// Recur(0, 'on', queue, jid, klass, data, now, 'interval', second, offset, [priority p], [tags t], [retries r])
// Put a recurring job in this queue
func (q *Queue) Recur(jid, klass string, data interface{}, interval, offset, priority int, tags []string, retries int) (string, error) {
  if jid == "" {
    jid = generateJID()
  }
  if interval == -1 {
    interval = 0
  }
  if offset == -1 {
    offset = 0
  }
  if priority == -1 {
    priority = 0
  }
  if retries == -1 {
    retries = 5
  }

  return redis.String(q.cli.Do(
    "recur", 0, "on", q.Name, jid, klass,
    data, timestamp(), "interval",
    interval, offset, "priority", priority,
    "tags", marshal(tags), "retries", retries))
}
