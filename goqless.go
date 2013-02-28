// reference: https://github.com/seomoz/qless-py
package goqless

import (
  "bytes"
  "crypto/rand"
  "crypto/sha1"
  "encoding/json"
  "fmt"
  "github.com/garyburd/redigo/redis"
  mrand "math/rand"
  "os"
  "strconv"
  "time"
  "unicode"
  "unicode/utf8"
)

// type Opts map[string]interface{}

// func (o Opts) Get(name string, dfault interface{}) interface{} {
//   if v, ok := o[name]; ok {
//     return v
//   }
//   return dfault
// }

// represents a string slice with special json unmarshalling
type StringSlice []string

func (s *StringSlice) UnmarshalJSON(data []byte) error {
  // because tables and arrays are equal in LUA,
  // an empty array would be presented as "{}".
  if bytes.Equal(data, []byte("{}")) {
    *s = []string{}
    return nil
  }

  return json.Unmarshal(data, s)
}

// Generates a jid
func generateJID() string {
  hasher := sha1.New()
  uuid := make([]byte, 16)
  n, err := rand.Read(uuid)
  if err != nil || n != len(uuid) {
    src := mrand.NewSource(time.Now().UnixNano())
    r := mrand.New(src)
    for n, _ := range uuid {
      uuid[n] = byte(r.Int())
    }
  }

  hasher.Write(uuid)
  return fmt.Sprintf("%x", hasher.Sum(nil))
}

// returns a timestamp used in LUA calls
func timestamp() int64 {
  return time.Now().UTC().Unix()
}

// returns a worker name for this machine/process
func workerName() string {
  hn, err := os.Hostname()
  if err != nil {
    hn = os.Getenv("HOSTNAME")
  }

  if hn == "" {
    hn = "localhost"
  }

  return fmt.Sprintf("%s-%d", hn, os.Getpid())
}

// makes the first character of a string upper case
func ucfirst(s string) string {
  if s == "" {
    return ""
  }
  r, n := utf8.DecodeRuneInString(s)
  return string(unicode.ToUpper(r)) + s[n:]
}

// marshals a value. if the value happens to be
// a string or []byte, just return it.
func marshal(i interface{}) []byte {
  switch v := i.(type) {
  case []byte:
    return v
  case string:
    return []byte(v)
  }

  byts, err := json.Marshal(i)
  if err != nil {
    return nil
  }
  return byts
}

// Bool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err. Otherwise Bool converts the
// reply to boolean as follows:
//
//  Reply type      Result
//  integer         value != 0, nil
//  bulk            strconv.ParseBool(reply) or r != "False", nil
//  nil             false, ErrNil
//  other           false, error
func Bool(reply interface{}, err error) (bool, error) {
  if err != nil {
    return false, err
  }
  switch reply := reply.(type) {
  case int64:
    return reply != 0, nil
  case []byte:
    r := string(reply)
    b, err := strconv.ParseBool(r)
    if err != nil {
      return r != "False", nil
    }
    return b, err
  case nil:
    return false, redis.ErrNil
  case redis.Error:
    return false, reply
  }
  return false, fmt.Errorf("redigo: unexpected type for Bool, got type %T", reply)
}
