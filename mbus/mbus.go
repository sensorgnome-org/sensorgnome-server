// simple message bus on pub/sub model, with these features:
// - publish never blocks waiting for subscribers
// - messages are received from a channel, which gets closed when the messagebus
//   is destroyed
// - for any subscriber, messages are received in the order they were published
// - delivery (to a subscriber's queue) is guaranteed;
// - uses one goroutine per subscriber plus one goroutine for the message bus
// - messages are stored by pointer, so queues are lightweight
//
// Message Flow:
//
// publisher ----+                      +-------> subscriber queue (1) --[viewer]--> chan for subscriber 1
//               |                      |
//               v                      |
// publisher -> publish queue -----[bcaster]----> subscriber queue (2) --[viewer]--> chan for subscriber 2
//               ^                      |
//               |                      |
// publisher ----+                      +-------> subscriber queue (3) --[viewer]--> chan for subscriber 3
//
// [bcaster] is a goroutine to move (and replicate) each message from the publish queue to the queues of all interested subscribers
// [viewer] is a goroutine to move messages from a subscriber's queue to its channel
//

package mbus

import (
	"github.com/foize/go.fifo"
	"sync"
)

// The type used for Message topics.
type Topic string

// Messages have a topic and an arbitrary object.
type Msg struct {
	Topic Topic
	Msg   interface{}
}

// Queues hold pointers to messages, to avoid copying
type PMsg *Msg

// a type representing a subscriber
// The client who subscribes receives a `<-chan Msg`, which also
// serves as a handle to this (opaque) structure.

type subr struct {
	wants map[Topic]bool // topics of messages wanted by subscriber
	lock  sync.Mutex     // lock for the `wants` map
	msgs  *fifo.Queue    // fifo of messages published and wanted (at the time of publication)
	// but not yet read from the message channel by the client
	send     chan PMsg // channel on which subscriber receives messages
	haveMsgs chan bool // channel on which carrier waits to learn there are msgs to distribute
	// when this channel closes, the subscriber's viewer goroutine terminates
}

// it would be nice to define `type MsgChan chan Msg` and then
// create a read-only variant, but go doesn't currently allow this;
// see: https://github.com/golang/go/issues/21953

// a type representing the message bus
type Mbus struct {
	msgs     *fifo.Queue           // queue of pointers to messages published but not yet distributed
	subs     map[<-chan PMsg]*subr // keep track of subscribers by the channel we return to them
	subsLock sync.Mutex            // lock for subscriber modifications
	wakeLock sync.Mutex            // lock for the waking up the distributor channel
	haveMsgs chan bool             // channel distributor waits on to indicate there are messages to distribute
	// when this channel closes, the message bus is terminated and all subscriber goroutines ended
}

// constructor for a message bus
func NewMbus() (mb Mbus) {
	mb = Mbus{msgs: fifo.NewQueue(), subs: make(map[<-chan PMsg]*subr, 10), haveMsgs: make(chan bool, 1)}
	// start the goroutine to distribute messages from the publish queue to the
	// subscriber queues
	go mb.bcaster()
	return
}

// create a subscriber to a topic of interest
func (mb *Mbus) Sub(topic Topic) (sc <-chan PMsg) {
	mb.subsLock.Lock()
	defer mb.subsLock.Unlock()
	sub := &subr{wants: make(map[Topic]bool, 10), msgs: fifo.NewQueue(), send: make(chan PMsg), haveMsgs: make(chan bool, 1)}
	sub.wants[topic] = true
	sc = sub.send
	mb.subs[sc] = sub
	// start the goroutine that moves messages from the queue (where the bcaster puts them)
	// to the subscriber's message channel
	go sub.viewer()
	return
}

// add a topic to an existing subscriber
// Returns true on success, false otherwise
func (mb *Mbus) Add(c <-chan PMsg, topic Topic) bool {
	mb.subsLock.Lock()
	defer mb.subsLock.Unlock()
	if s, ok := mb.subs[c]; ok {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.wants[topic] = true
		return true
	}
	return false
}

// remove a topic from an existing subscriber message channel
// returns true on sucess, false otherwise.  Note that a message
// channel can exist without any subscriptions.  Reads from it
// will then always block.
func (mb *Mbus) Drop(c <-chan PMsg, topic Topic) bool {
	mb.subsLock.Lock()
	defer mb.subsLock.Unlock()
	if s, ok := mb.subs[c]; ok {
		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.wants, topic)
		return true
	}
	return false
}

// get number of topics subscribed to
// if c is not a valid message channel for mb,
// return -1
func (mb *Mbus) NumTopics(c <-chan PMsg) int {
	mb.subsLock.Lock()
	defer mb.subsLock.Unlock()
	if s, ok := mb.subs[c]; ok {
		return len(s.wants)
	}
	return -1
}

// return topics subscribed to as a slice of strings
func (mb *Mbus) Topics(c <-chan PMsg) []Topic {
	mb.subsLock.Lock()
	defer mb.subsLock.Unlock()
	if s, ok := mb.subs[c]; ok {
		rv := make([]Topic, len(s.wants))
		i := 0
		for t, _ := range s.wants {
			rv[i] = t
			i++
		}
		return rv
	}
	return make([]Topic, 0)
}

// publish a message
// The message is added to the publisher queue and then a
// bool is sent down the haveMsgs channel to (possibly) wake
// the distor goroutine.
func (mb *Mbus) Pub(m Msg) {
	// add message to publisher queue (locking happens within
	// the fifo code)
	mb.msgs.Add(PMsg(&m))
	// make sure the distributor has a wake-up message in
	// its haveMsgs fifo
	mb.wakeLock.Lock()
	defer mb.wakeLock.Unlock()
	// ensure the haveMsgs channel contains an item
	// to force the distor to wakeup
	if len(mb.haveMsgs) == 0 {
		mb.haveMsgs <- true
	}
}

// goroutine to distribute messages from the publish queue to
// the input queue of all interested subscribers
func (mb *Mbus) bcaster() {
	for _ = range mb.haveMsgs {
		// whenever a receive from haveMsgs succeeds, it means
		// there might be messages in the queue
		for mb.msgs.Len() > 0 {
			msg := mb.msgs.Next().(PMsg)
			// lock the subscribers list and look for those wanting
			// this message topic
			mb.subsLock.Lock()
			for _, sub := range mb.subs {
				sub.lock.Lock()
				if sub.wants[msg.Topic] {
					sub.msgs.Add(msg)
					// ensure the haveMsgs channel contains an item
					// to force the carrier to wake up
					if len(sub.haveMsgs) == 0 {
						sub.haveMsgs <- true
					}
				}
				sub.lock.Unlock()
			}
			mb.subsLock.Unlock()
		}
	}
}

// goroutine to send desired messages to subscriber's channel
func (sub *subr) viewer() {
	for _ = range sub.haveMsgs {
		// whenever a receive from haveMsgs succeeds, it means
		// there might be messages in the queue
		for sub.msgs.Len() > 0 {
			msg := sub.msgs.Next().(PMsg)
			sub.send <- msg
		}
	}
}
