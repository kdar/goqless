package goqless

// import (
//   "fmt"
//   // "github.com/garyburd/redigo/redis"
//   // "time"
// )

// func ExampleGoqless_1() {
//   c, err := Dial("", "6379")
//   if err != nil {
//     panic(err)
//   }
//   defer c.Close()

//   jid := "f5b66400bf027c191ddb80a85785b03eb9765456"
//   c.Track(jid)
//   q := c.Queue("goqless_testing_queue")

//   putreply, err := q.Put(jid, "dunno", `{"hey": "there"}`, -1, -1, []string{}, -1, []string{})
//   fmt.Println("Put:", putreply, err)
//   //fmt.Println(q.Recur(jid, "dunno", `{"hey": "there"}`, 5, 0, 0, []string{}, 1))

//   jobs, err := q.Pop(1)
//   fmt.Printf("Pop: %s %s %v\n", jobs[0].Queue, jobs[0].Data, err)
//   fmt.Print("Complete: ")
//   fmt.Println(jobs[0].Complete())

//   //wg.Wait()

//   // Output:
//   // Put: f5b66400bf027c191ddb80a85785b03eb9765456 <nil>
//   // Pop: goqless_testing_queue map[hey:there] <nil>
//   // Complete: complete <nil>
// }

// func ExampleGoqless_2() {
//   c, err := Dial("", "6379")
//   if err != nil {
//     panic(err)
//   }
//   defer c.Close()

//   jid := "f5b66400bf027c191ddb80a85785b03eb9765457"
//   c.Track(jid)
//   q := c.Queue("goqless_testing_queue")

//   data := struct {
//     Str string
//   }{
//     "a string",
//   }

//   putreply, err := q.Put(jid, "dunno", data, -1, -1, []string{}, -1, []string{})
//   fmt.Println("Put:", putreply, err)
//   //fmt.Println(q.Recur(jid, "dunno", `{"hey": "there"}`, 5, 0, 0, []string{}, 1))

//   jobs, err := q.Pop(1)
//   fmt.Printf("Pop: %s %v %v\n", jobs[0].Queue, jobs[0].Data, err)
//   fmt.Print("Fail: ")
//   fmt.Println(jobs[0].Fail("justbecause", "i said so"))

//   // Output:
//   // Put: f5b66400bf027c191ddb80a85785b03eb9765457 <nil>
//   // Pop: goqless_testing_queue map[Str:a string] <nil>
//   // Fail: true <nil>
// }
