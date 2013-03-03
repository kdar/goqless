package goqless

import (
  "github.com/garyburd/redigo/redis"
  "reflect"
  "strings"
  "time"
  // "encoding/json"
)

var (
  JOBSTATES = []string{"stalled", "running", "scheduled", "depends", "recurring"}
)

type History struct {
  Popped int64
  Q      string
  Put    int64
  Worker string
}

type Job struct {
  Expires      int64
  Dependents   StringSlice
  Tracked      bool
  Tags         StringSlice
  Jid          string
  Retries      int
  Data         interface{}
  Queue        string
  State        string
  Remaining    int
  Failure      interface{}
  History      []History
  Dependencies interface{}
  Klass        string
  Priority     int
  Worker       string

  cli *Client
}

func NewJob(cli *Client) *Job {
  return &Job{
    Expires:      time.Now().Add(time.Hour * 60).UTC().Unix(), // hour from now
    Dependents:   nil,                                         // []interface{}{},
    Tracked:      false,
    Tags:         nil,
    Jid:          generateJID(),
    Retries:      5,
    Data:         nil,
    Queue:        "mock_queue",
    State:        "running",
    Remaining:    5,
    Failure:      nil,
    History:      nil, // []interface{}{},
    Dependencies: nil,
    Klass:        "Job",
    Priority:     0,
    Worker:       "mock_worker",
    cli:          cli,
  }
}

func (j *Job) Client() *Client {
  return j.cli
}

func (j *Job) SetClient(cli *Client) {
  j.cli = cli
}

// Move this from it's current queue into another
func (j *Job) Move() (string, error) {
  return redis.String(j.cli.Do("put", 1, j.Queue, j.Jid, j.Klass, marshal(j.Data), timestamp(), 0))
}

// Fail(0, id, worker, type, message, now, [data])
// Fail this job
// return success, error
func (j *Job) Fail(typ, message string) (bool, error) {
  return Bool(j.cli.Do("fail", 0, j.Jid, j.Worker, typ, message, timestamp(), marshal(j.Data)))
}

// Heartbeat(0, id, worker, now, [data])
// Heartbeats this job
// return success, error
func (j *Job) Heartbeat() (bool, error) {
  return Bool(j.cli.Do("heartbeat", 0, j.Jid, j.Worker, timestamp(), marshal(j.Data)))
}

// Complete(0, jid, worker, queue, now, data, ['next', n, [('delay', d) | ('depends', '["jid1","jid2",...]')])
// Completes this job
// returns state, error
func (j *Job) Complete() (string, error) {
  return redis.String(j.cli.Do("complete", 0, j.Jid, j.Worker, j.Queue, timestamp(), marshal(j.Data)))
}

// Cancel(0, id)
// Cancels this job
func (j *Job) Cancel() {
  j.cli.Do("cancel", 0, j.Jid)
}

// Track(0, 'track', jid, now, tag, ...)
// Track this job
func (j *Job) Track() (bool, error) {
  return Bool(j.cli.Do("track", 0, "track", j.Jid, timestamp(), ""))
}

// Track(0, 'untrack', jid, now)
// Untrack this job
func (j *Job) Untrack() (bool, error) {
  return Bool(j.cli.Do("track", 0, "untrack", j.Jid, timestamp()))
}

// Tag(0, 'add', jid, now, tag, [tag, ...])
func (j *Job) Tag(tags ...interface{}) (string, error) {
  args := []interface{}{0, "add", j.Jid, timestamp()}
  args = append(args, tags...)
  return redis.String(j.cli.Do("tag", args...))
}

// Tag(0, 'remove', jid, now, tag, [tag, ...])
func (j *Job) Untag(tags ...interface{}) (string, error) {
  args := []interface{}{0, "remove", j.Jid, timestamp()}
  args = append(args, tags...)
  return redis.String(j.cli.Do("tag", args...))
}

// Retry(0, jid, queue, worker, now, [delay])
func (j *Job) Retry(delay int) (string, error) {
  return redis.String(j.cli.Do("retry", 0, j.Jid, j.Queue, j.Worker, delay))
}

// Depends(0, jid, 'on', [jid, [jid, [...]]])
func (j *Job) Depend(jids ...interface{}) (string, error) {
  args := []interface{}{0, j.Jid, "on"}
  args = append(args, jids...)
  return redis.String(j.cli.Do("depends", args...))
}

// Depends(0, jid, 'off', ('all' | [jid, [jid, [...]]]))
func (j *Job) Undepend(jids ...interface{}) (string, error) {
  args := []interface{}{0, j.Jid, "off"}
  args = append(args, jids...)
  return redis.String(j.cli.Do("depends", args...))
}

type RecurringJob struct {
  Tags     StringSlice
  Jid      string
  Retries  int
  Data     interface{}
  Queue    string
  Interval int
  Count    int
  Klass    string
  Priority int

  cli *Client
}

func NewRecurringJob(cli *Client) *RecurringJob {
  return &RecurringJob{cli: cli}
}

// example: job.Update(map[string]interface{}{"priority": 5})
// options:
//   priority int
//   retries int
//   interval int
//   data interface{}
//   klass string
func (r *RecurringJob) Update(opts map[string]interface{}) {
  args := []interface{}{0, "update", r.Jid}

  vOf := reflect.ValueOf(r).Elem()
  for key, value := range opts {
    key = strings.ToLower(key)
    v := vOf.FieldByName(ucfirst(key))
    if v.IsValid() {
      setv := reflect.ValueOf(value)
      if key == "data" {
        setv = reflect.ValueOf(marshal(value))
      }
      v.Set(setv)
      args = append(args, key, value)
    }
  }

  r.cli.Do("recur", args...)
}

func (r *RecurringJob) Cancel() {
  r.cli.Do("recur", 0, "off", r.Jid)
}

func (r *RecurringJob) Tag(tags ...interface{}) {
  args := []interface{}{0, "tag", r.Jid}
  args = append(args, tags...)
  r.cli.Do("recur", args...)
}

func (r *RecurringJob) Untag(tags ...interface{}) {
  args := []interface{}{0, "untag", r.Jid}
  args = append(args, tags...)
  r.cli.Do("recur", args...)
}
