package main

// simple test of the mbus package

import (
	"math/rand"
	"fmt"
	"strconv"
	"sync"
	"time"
	"./mbus"
)

var wg sync.WaitGroup

// consumer id receives messages whose topic (as an integer) is divisible by id
func consumer (id int, c <-chan mbus.PMsg) {
	for x := range c {
		fmt.Printf("%03d got %s = %s\n", id, x.Topic, x.Msg)
		wg.Done()
		time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond)
	}
}

// publisher id generates n messages with random topics from 1 to 100
func producer (id int, n int, mb *mbus.Mbus) {
	// publish 100 messages on random topics between 1 and 100
	for i:= 1; i <= n; i++ {
		n := rand.Intn(100) + 1
		// count the number of consumers who should receive the message
		nm := 0
		for j := 1; j <= 30; j++ {
			if n % j == 0 {
				nm++
			}
		}
		m := mbus.Msg{mbus.Topic(strconv.Itoa(n)), fmt.Sprintf("#%d from producer %d", i, id)}
		fmt.Printf("published %s = %s\n", m.Topic, m.Msg)
		wg.Add(nm)
		mb.Pub(m)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	mb := mbus.NewMbus()
	// generate 30 random consumers; consumer i subscribes to
	// messages whose topic is an integer multiple of i <= 100
	for i:= 1; i <= 30; i++ {
		c := mb.Sub(mbus.Topic(strconv.Itoa(i)), mbus.Topic(strconv.Itoa(2*i)))
		for j:= i*3; j <= 100; j += i {
			mb.Add(c, mbus.Topic(strconv.Itoa(j)))
		}
		if i == 12 {
			fmt.Printf("consumer %d wants these %d topics:\n", i, mb.NumTopics(c))
			for _,t := range mb.Topics(c) {
				fmt.Printf("   %s\n", t)
			}
		}
		go consumer(i, c)
	}
	for i:= 1; i <= 30; i++ {
		go producer(i, i, &mb)
	}
	// can do only one of the two following
	if rand.Intn(2) == 1 {
		// sleep for one second so producers can add to wait group
		time.Sleep(time.Second)
		fmt.Println("Slept for 1s; now waiting for all messages to be received")
		wg.Wait()
	} else {
		// sleep a shorter time, then close
		time.Sleep(time.Millisecond * 10)
		fmt.Println("Slept for 10 ms; now closing mbus")
		mb.Close()
	}
}
