package main

import (
	//	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/jbrzusto/mbus"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	//	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// customization constants
const (
	AddressRegServer      = "localhost:59026" // TCP interface: port on which registration exchanges happen
	AddressStatusServer   = "localhost:59025" // TCP interface:port on which status requests are answered
	AddressTrustedDgram   = ":59023"          // UDP interface:port on which we receive unsigned messages from trusted sources (e.g. localhost)
	TrustedStreamPort     = "59024"
	AddressTrustedStream  = "localhost:" + TrustedStreamPort // TCP interface:port on which we receive messages from trusted sources (e.g. SGs connected via ssh)
	AddressUntrustedDgram = ":59022"                         // UDP interface:port on which we receive messages from untrusted sources
	// AddressRegServer      = "localhost:59036" // TCP interface: port on which registration exchanges happen
	// AddressStatusServer   = "localhost:59035" // TCP interface:port on which status requests are answered
	// AddressTrustedDgram   = ":59033"          // UDP interface:port on which we receive unsigned messages from trusted sources (e.g. localhost)
	// TrustedStreamPort     = "59034"
	// AddressTrustedStream  = "localhost:" + TrustedStreamPort                                           // TCP interface:port on which we receive messages from trusted sources (e.g. SGs connected via ssh)
	// AddressUntrustedDgram = ":59032"                                                                           // UDP interface:port on which we receive signed messages from untrusted sources
	ConnectionSemPath     = "/dev/shm"                                                                         // directory where sshd maintains semaphores indicating connected SGs
	ConnectionSemRE       = "sem.(" + SernoRE + ")"                                                            // regular expression for matching SG semaphores (capture group is serno)
	CryptoAuthKeysPath    = CryptoKeyPath + "/authorized_keys"                                                 // sshd authorized_keys file for remote SGs
	CryptoKeyPath         = "/home/sg_remote/.ssh"                                                             // where crypto keys for remote SGs are stored
	MotusControlPath      = "/home/sg_remote/sgdata.ssh"                                                       // control path for multiplexing port mappings to sgdata.motus.org
	MotusAuthUser         = `https://motus.org/api/user/validate?json={"date":"%s","login":"%s","pword":"%s"}` // URL to validate motus user and return authorizations
	MotusGetProjectsUrlT  = `https://motus.org/api/projects?json={"date":"%s"}`                                // URL for motus info on projects
	MotusGetReceiversUrlT = `https://motus.org/api/receivers/deployments?json={"date":"%s","status":2}`        // URL for motus info on receivers
	MotusMinLatency       = 10                                                                                 // minimum time (minutes) between queries to the motus metadata server
	MotusSyncTemplate     = "/sgm_local/sync/method=%d,serno=%s"                                               // template for file touched on sgdata.motus.org to cause sync; %d=port, %s=serno
	MotusUserKey          = "/home/sg_remote/.ssh/id_ed25519_sgorg_sgdata"                                     // ssh key to use for sync on sgdata.motus.org
	MotusUser             = "sg@sgdata.motus.org"                                                              // user on sgdata.motus.org; this is who ssh makes us be
	SernoRE               = "SG-[0-9A-Za-z]{12}"                                                               // regular expression matching SG serial number
	SGDBFile              = "/home/sg_remote/sg_remote.sqlite"                                                 // sqlite database with receiver info
	ShortTimestampFormat  = "Jan _2 15:04"                                                                     // timestamp format for sync times etc. on status page
	StatusPageMinLatency  = 5                                                                                  // minimum latency (seconds) between status page updates
	StatusPagePath        = "/home/johnb/src/sensorgnome-server/website/content/status/index.md"               // path to generated page (needs group write permission and ownership by sg_remote group)
	SyncTimeDir           = "/home/sg_remote/last_sync"                                                        // directory with one file per SG; mtime is last sync time
	SyncWaitHi            = 90                                                                                 // maximum time between syncs of a receiver (minutes)
	SyncWaitLo            = 30                                                                                 // minimum time between syncs of a receiver (minutes)
	TrustedIPAddrRE       = `209\.183\.24\.36:[0-9]+`                                                          // hard-wired trusted network address for registration
	TunnelPortMax         = 49999                                                                              // maximum SG tunnel port we assign
	TunnelPortMin         = 40000                                                                              // minimum SG tunnel port we assign
)

// regular expression matching a trusted net.Addr.String()
var TrustedIPAddrRegexp = regexp.MustCompile(TrustedIPAddrRE)

// The type for messages.
type SGMsg struct {
	ts     time.Time // timestamp; if 0, means not set
	sender string    // typically the SG serial number, but can be "me" for internally generated
	text   string    // typically a JSON- or CSV- formatted message
}

// type representing an SG message topic
type MsgTopic string

// Message Topics
// These are typically the first character of the message from the SG,
// but we add some synthetic ones generated by the system
const (
	MsgSGDisconnect  = "0" // connected via ssh
	MsgSGConnect     = "1" // disconnected from ssh
	MsgSGSync        = "2" // data sync with motus.org was launched
	MsgSGSyncPending = "3" // data sync with motus.org has been scheduled for a future time
	MsgSGActivate    = "4" // receiver has connected *and* had its info read from DB
	MsgStatusChange  = "5"
	MsgGPS           = "G" // from SG: GPS fix
	MsgMachineInfo   = "M" // from SG: machine information
	MsgTimeSync      = "C" // from SG: time sync
	MsgDeviceSetting = "S" // from SG: setting for a device
	MsgDevAdded      = "A" // from SG: device was added
	MsgDevRemoved    = "R" // from SG: device was removed
	MsgTag           = "p" // from SG: tag was detected
)

// global message bus
//
// all messages are published on this bus under one of the MsgTopics
var Bus mbus.Mbus

// type representing an SG serial number
type Serno string

// regular expression matching an SG serial number
var SernoRegexp = regexp.MustCompile(SernoRE)

// an SG we have seen recently
type ActiveSG struct {
	Serno      Serno     // serial number; e.g. "SG-1234BBBK9812"
	TsConn     time.Time // time at which connected
	TsDisConn  time.Time // time at which last disconnected
	TsLastSync time.Time // time at which last synced with motus
	TsNextSync time.Time // time at which next to be synced with motus
	TunnelPort int       // ssh tunnel port, if applicable
	Connected  bool      // actually connected?  once we've seen a receiver, we keep this struct in memory,
	// but set this field to false when it disconnects
	lock sync.Mutex // lock for any read or write access to fields in this struct
}

// map from serial number to pointer to active SG structure
//
// We use a sync.Map to allow safe multi-threaded access to
// the pointers. Also, we never remove entries from the map, so a
// pointer to an ActiveSG is always valid once created.
var activeSGs sync.Map

// read one line at a time from an io.Reader
type LineReader struct {
	dest   *[]byte   // where a single line is written
	buf    []byte    // buffer for reading
	bufp   int       // position of next character in buf to use
	buflen int       // number of characters left in buffer
	rdr    io.Reader // connection being read from
}

// constructor
func NewLineReader(rdr net.Conn, dest *[]byte) *LineReader {
	r := new(LineReader)
	r.rdr = rdr
	r.dest = dest
	r.buf = make([]byte, len(*dest))
	r.bufp = 0
	r.buflen = 0
	return r
}

// get a line into the dest buffer.  The trailing newline is stripped.
// len(dest) is set to the number of bytes in the string.

func (r *LineReader) getLine() (err error) {
	n := 0
	r.buf = r.buf[:cap(r.buf)]
	var dst = (*r.dest)[:cap(*r.dest)]
	for n < len(dst) {
		for r.bufp >= r.buflen {
			// note: Read(buf) reads at most len(buf), not cap(buf)!
			m, err := r.rdr.Read(r.buf)
			if m == 0 && err != nil {
				return err
			}
			r.bufp = 0
			r.buflen = m
		}
		c := r.buf[r.bufp]
		if c == '\n' {
			r.bufp++
			break
		}
		dst[n] = c
		n++
		r.bufp++
	}
	*r.dest = (*r.dest)[:n]
	return nil
}

// Handle messages from a trusted stream and send them
// the dst channel.  The first line in a trusted stream
// provides the sender, which is used in the SGMsgs generated
// from all subsequent lines.
// FIXME: add context.Context
func handleTrustedStream(conn net.Conn) {
	buff := make([]byte, 4096)
	var addr = conn.RemoteAddr()
	var lr = NewLineReader(conn, &buff)
	_ = lr.getLine()
	var sender string = string(buff)
	for {
		err := lr.getLine()
		if err != nil {
			fmt.Printf("connection from %s@%s closed\n", sender, addr)
			return
		}
		// send a message on the bus; the topic is the first character of the message from the SG
		Bus.Pub(mbus.Msg{mbus.Topic(string(buff[0])), SGMsg{ts: time.Now(), sender: sender, text: string(buff)}})
	}
}

// listen for trusted streams and dispatch them to a handler
func TrustedStreamSource(ctx context.Context, address string) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		print("failed to resolve address " + address)
		return
	}
	srv, err := net.ListenTCP("tcp", addr)
	if err != nil {
		print("failed to listen on " + address)
		return
	}
	defer srv.Close()
	for {
		conn, err := srv.AcceptTCP()
		if err != nil {
			// handle error
			print("problem accepting connection")
			return
		}
		go handleTrustedStream(net.Conn(conn))
	}
	select {
	case <-ctx.Done():
	}
}

// Listen for datagrams on either a trusted or untrusted port.
// Datagrams from the trusted port are treated as authenticated.
// Datagrams from an untrusted port have their signature checked
// and are discarded if this is not valid.
// Datagrams are passed to the dst channel as SGMsgs.
func DgramSource(ctx context.Context, address string, trusted bool) {
	pc, err := net.ListenPacket("udp", address)
	if err != nil {
		print("failed to listen on port " + address)
		return
	}
	defer pc.Close()
	doneChan := make(chan error, 1)
	buff := make([]byte, 1024)
	go func() {
		for {
			_, addr, err := pc.ReadFrom(buff)
			if err != nil {
				doneChan <- err
				return
			}
			var prefix = ""
			if trusted {
				prefix = "not "
			}

			fmt.Printf("Got %s from %s %strusted\n", buff, addr, prefix)
		}
	}()
	select {
	case <-ctx.Done():
		fmt.Println("cancelled")
		err = ctx.Err()
	case err = <-doneChan:
	}
}

// DEBUG: A goroutine to dump Msgs
func messageDump() {
	evt := Bus.Sub("*")
	go func() {
		defer evt.Unsub("*")
		for msg := range evt.Msgs() {
			if msg.Msg == nil {
				continue
			}
			m := msg.Msg.(SGMsg)
			fmt.Printf("%s: %s,%s,%s\n", msg.Topic, m.ts, m.sender, m.text)
		}
	}()
}

func unixtime(ts time.Time) float64 {
	return float64(ts.UnixNano()) / 1.0E9
}

// Goroutine that records (some) messages to a
// table called "messages" in the global DB.
func DBRecorder() {
	stmt, err := DB.Prepare("INSERT INTO messages (ts, sender, message) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	// subscribe to topics of interest
	evt := Bus.Sub("*")
	go func() {
		// create closure that uses stmt, db
		defer stmt.Close()
		defer evt.Unsub("*")
		for msg := range evt.Msgs() {
			if msg.Msg == nil {
				continue
			}
			m := msg.Msg.(SGMsg)
			ts, sender, text := m.ts, m.sender, m.text
			// fill in defaults
			if ts.IsZero() {
				ts = time.Now()
			}
			if text == "" {
				text = string(msg.Topic)
			}
			// record timestamp in DB as double seconds;
			_, err := stmt.Exec(unixtime(ts), sender, text)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()
}

// simple SQL query
//
// st is a prepared statement; pars are parameters to it, and res is
// pointers to result values.  Attempts to run the statement with
// the given parameters, and store the results from the *first* row
// into the result values.
//
// if len(res) == 0, the statement is run using sql.Stmt.Exec,
// otherwise, using sql.Stmt.Query
//
// returns true if len(res) == 0 and no errors were encountered,
// or if len(res) != 0) and at least one row was returned

func SQL(q dbQuery, pars []interface{}, res []interface{}) (rv bool) {
	rv = false
	if q >= DBQ_num_queries {
		return false
	}
	st := dbQueries[q]
	if len(res) > 0 {
		rows, err := st.Query(pars...)
		if err == nil {
			if rows.Next() {
				rows.Scan(res...)
				rv = true
			}
			rows.Close()
		}
	} else {
		_, err := st.Exec(pars...)
		if err == nil {
			rv = true
		}
	}
	return
}

// wrap an arbitrary set of interface{} objects into a slice
//
// 'c' for combine, as in R
func c(v ...interface{}) []interface{} {
	// trivial implementation, courtesy of `...` semantics
	return v
}

// update an ActiveSG record from the global database DB
//
// return the object pointer to allow chaining
// errors in getting records from the DB are ignored
func (sg *ActiveSG) FromDB() *ActiveSG {
	// get its tunnel port from the receivers table
	var (
		t  int
		ts float64
	)
	if SQL(DBQGetTunnelPort, c(sg.Serno), c(&t)) &&
		SQL(DBQGetTsLastSync, c(sg.Serno), c(&ts)) {
		sg.lock.Lock()
		defer sg.lock.Unlock()
		sg.TunnelPort = t
		sg.TsLastSync = time.Unix(0, int64(ts*1E9))
	}
	return sg
}

// generate messages for SG connection / disconnection events
//
// Watch directory `dir` for creation / deletion of files representing
// connected SGs. Files representing SGs are those matching the first
// capture group of `sgRE`.  After establishing a watch goroutine,
// events are generated for any files already in `dir`, using the file
// mtime.  This creates a race condition under which we might generate
// two SGConnect events for the same SG; subscribers need to account
// for this.

func ConnectionWatcher(ctx context.Context, dir string, sgRE string) {
	re := regexp.MustCompile(sgRE)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = watcher.Add(dir)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				parts := re.FindStringSubmatch(event.Name)
				if parts != nil {
					msg := mbus.Msg{MsgSGConnect, SGMsg{sender: parts[1], ts: time.Now()}}
					if event.Op&fsnotify.Remove == fsnotify.Remove {
						msg.Topic = mbus.Topic(MsgSGDisconnect)
					}
					Bus.Pub(msg)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			case <-ctx.Done():
				return
			}
		}
	}()
	// generate "connection" events from existing connection semaphores
	// (these are receivers already connected when this sensorgnomeServer started)
	files, err := ioutil.ReadDir(dir)
	if err == nil {
		for _, finfo := range files {
			parts := re.FindStringSubmatch(finfo.Name())
			if parts != nil {
				Bus.Pub(mbus.Msg{MsgSGConnect, SGMsg{sender: parts[1], ts: finfo.ModTime()}})
			}
		}
	}
}

// goroutine to maintain a list of active SGs and their status
//
// publishes an MsgStatusChange message when anything changes

func SGMinder() {
	evt := Bus.Sub(MsgSGConnect, MsgSGDisconnect, MsgSGSync, MsgSGSyncPending)
	go func() {
		defer evt.Unsub("*")
		for msg := range evt.Msgs() {
			t, m := MsgTopic(msg.Topic), msg.Msg.(SGMsg)
			serno := Serno(m.sender)
			sgp, ok := activeSGs.Load(serno)

			if !ok {
				// not in current list, so populate what is known from DB
				if t != MsgSGConnect {
					panic("received non-connect message for not-yet-seen SG" + serno)
				}
				newsg := (&ActiveSG{Serno: serno, TsConn: m.ts, Connected: true}).FromDB()
				sgp = newsg
				activeSGs.Store(serno, sgp)
			}
			Bus.Pub(mbus.Msg{MsgSGActivate, SGMsg{sender: string(serno)}})
			sg := sgp.(*ActiveSG)
			sg.lock.Lock()
			switch t {
			case MsgSGConnect:
				sg.TsConn = m.ts
				sg.Connected = true
			case MsgSGDisconnect:
				sg.TsDisConn = m.ts
				sg.Connected = false
			case MsgSGSync:
				sg.TsLastSync = m.ts
			case MsgSGSyncPending:
				sg.TsNextSync = m.ts
			}
			sg.lock.Unlock()
			Bus.Pub(mbus.Msg{MsgStatusChange, nil})
		}
	}()
}

// manage repeated sync jobs for a single SG
// emit a message each time a receiver sync is launched
func SyncWorker(ctx context.Context, serno Serno) {
	// grab receiver info pointer
	sgp, ok := activeSGs.Load(serno)
	if !ok {
		return // should never happen!
	}
	sg := sgp.(*ActiveSG)
	sg.lock.Lock()
	cp := fmt.Sprintf("-oControlPath=%s", MotusControlPath)
	pf := fmt.Sprintf("-R%d:localhost:%d", sg.TunnelPort, sg.TunnelPort)
	tf := fmt.Sprintf(MotusSyncTemplate, sg.TunnelPort, string(serno))
	sg.lock.Unlock()
	var wait *time.Timer
SyncLoop:
	for {
		// set up a wait uniformly distributed between lo and hi times
		delay := time.Duration(SyncWaitLo+rand.Int31n(SyncWaitHi-SyncWaitLo)) * time.Minute
		if wait == nil {
			wait = time.NewTimer(delay)
		} else {
			wait.Reset(delay)
		}
		Bus.Pub(mbus.Msg{MsgSGSyncPending, SGMsg{sender: string(serno), ts: time.Now().Add(delay)}})
		select {
		case synctime := <-wait.C:
			// if receiver is not still connected, end this goroutine
			sg.lock.Lock()
			quit := !sg.Connected
			sg.lock.Unlock()
			if quit {
				break SyncLoop
			}
			cmd := exec.Command("ssh", "-i", MotusUserKey, "-f", "-N", "-T",
				"-oStrictHostKeyChecking=no", "-oExitOnForwardFailure=yes", "-oControlMaster=auto",
				"-oServerAliveInterval=5", "-oServerAliveCountMax=3",
				cp, pf, MotusUser)
			err := cmd.Run()
			// ignoring error; it is likely just the failure to map an already mapped port
			cmd = exec.Command("ssh", "-i", MotusUserKey, "-oControlMaster=auto",
				cp, MotusUser, "touch", tf)
			err = cmd.Run()
			if err == nil {
				Bus.Pub(mbus.Msg{MsgSGSync, SGMsg{ts: synctime, sender: string(serno)}})
			} else {
				fmt.Println(err.Error())
			}

		case <-ctx.Done():
			wait.Stop()
			break SyncLoop
		}
	}
}

// manage motus data sync for SGs
//
// Subscribe to topic "SGEvent" on the global message bus.  Handle these like so:
//
// - `SGActivate`: start a SyncWorker (receiver-specific goroutine) that periodically starts a sync job to send new data
// to sgdata.motus.org.   Multiple `SGConnect` events for the same receiver are collapsed into the
// first one.  We need metadata for the receiver (e.g. tunnel port) which is why we subscribe to this message
// instead of to `SGConnect`
// - `SGDisconnect`: stop the asssociated SyncWorker
//

func SyncManager() {
	syncCancels := make(map[Serno]context.CancelFunc)
	evt := Bus.Sub(MsgSGActivate, MsgSGDisconnect)
	go func() {
		defer evt.Unsub("*")
	MsgLoop:
		for e := range evt.Msgs() {
			m := e.Msg.(SGMsg)
			serno := Serno(m.sender)
			_, have := syncCancels[serno]
			switch e.Topic {
			case MsgSGActivate:
				if have {
					continue MsgLoop
				}
				newctx, cf := context.WithCancel(context.Background())
				syncCancels[serno] = cf
				go SyncWorker(newctx, serno)
			case MsgSGDisconnect:
				if !have {
					continue MsgLoop
				}
				syncCancels[serno]()
				delete(syncCancels, serno)
			}
		}
		// closing, so shut down all sync goroutines
		for serno, sc := range syncCancels {
			sc()
			delete(syncCancels, serno)
		}
	}()
}

// global database pointer
var DB *sql.DB

// enum type for prepared queries
type dbQuery int

// query indexes by name
const (
	DBQGetTunnelPort   dbQuery = iota // get tunnel port by serno from receivers
	DBQGetTsLastSync                  // get last sync time by serno from messages
	DBQGetRegistration                // get registration by serno (tunnelPort, pubKey, privKey)
	DBQNewSG                          // insert a record with tunnelPort for new serno
	DBQNewSGKeys                      // update keys for an SG
	DBQ_num_queries                   // marks number of queries
)

// text of the queries; order must match that of constants above
var dbQueryText = [DBQ_num_queries]string{
	"SELECT tunnelPort FROM receivers WHERE serno=?",
	"SELECT max(ts) FROM messages WHERE sender = ? AND SUBSTR(message, 1, 1) == '2'",
	"SELECT tunnelPort, pubKey, privKey From receivers Where serno=?",
	"INSERT INTO receivers (serno, tunnelport) SELECT serno, tunnelPort FROM (SELECT ? AS serno, MIN(t1.tunnelport)+1 AS tunnelPort FROM receivers AS t1 LEFT JOIN receivers AS t2 ON t1.tunnelport=t2.tunnelport-1 WHERE t2.tunnelport IS NULL) where tunnelPort between " + strconv.Itoa(TunnelPortMin) + " and " + strconv.Itoa(TunnelPortMax),
	"update receivers set creationdate=?, pubkey=?, privkey=?, verified=? where serno=?"}

// global slice of prepared queries
var dbQueries [DBQ_num_queries]*sql.Stmt

// open/create the main database
//
// also prepares all parameterized queries

func OpenDB(path string) (db *sql.DB) {
	var err error
	db, err = sql.Open("sqlite3", SGDBFile)
	if err != nil {
		log.Fatal(err)
	}
	stmts := [...]string{
		`CREATE TABLE IF NOT EXISTS messages (
                    ts DOUBLE,
                    sender TEXT,
                    message TEXT
                )`,
		`CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts)`,
		`CREATE INDEX IF NOT EXISTS messages_sender ON messages(sender)`,
		`CREATE INDEX IF NOT EXISTS messages_sender_ts ON messages(sender, ts)`,
		`CREATE INDEX IF NOT EXISTS messages_sender_type_ts ON messages(sender, substr(message, 1, 1), ts)`,
		`CREATE TABLE IF NOT EXISTS receivers (
                 serno        TEXT UNIQUE PRIMARY KEY, -- only one entry per receiver
                 creationdate REAL,                    -- timestamp when this entry was created
                 tunnelport   INTEGER UNIQUE,          -- port used on server for reverse tunnel back to sensorgnome
                 pubkey       TEXT,                    -- unique public/private key pair used by sensorgnome to login to server
                 privkey      TEXT,
                 verified     INTEGER DEFAULT 0        -- has this SG been verified to belong to a real user?
                 )`,
		`CREATE INDEX IF NOT EXISTS receivers_tunnelport ON receivers(tunnelport)`,
		`CREATE TABLE IF NOT EXISTS deleted_receivers (
                 ts           REAL,                    -- deletion timestamp
                 serno        TEXT,                    -- possibly multiple entries per receiver
                 creationdate REAL,                    -- timestamp when this entry was created
                 tunnelport   INTEGER,                 -- port used on server for reverse tunnel back to sensorgnome
                 pubkey       TEXT,                    -- unique public/private key pair used by sensorgnome to login to server
                 privkey      TEXT,
                 verified     INTEGER DEFAULT 0        -- non-zero when verified
                 )`,
		`CREATE INDEX IF NOT EXISTS deleted_receivers_tunnelport ON deleted_receivers(tunnelport)`,
		`PRAGMA busy_timeout = 60000`} // set a very generous 1-minute timeout for busy wait

	for _, s := range stmts {
		_, err = db.Exec(s)
		if err != nil {
			log.Printf("error: %s\n", s)
			log.Fatal(err)
		}
	}

	// prepare all statements we'll use
	for i, q := range dbQueryText {
		dbQueries[i], err = db.Prepare(q)
		if err != nil {
			panic("Unable to prepare query: " + q)
		}
	}
	return
}

const (
	CMD_WHO = iota
	CMD_PORT
	CMD_SERNO
	CMD_JSON
	CMD_QUIT
)

// Handle status requests.
// The request is a one line format, such as "json\n".
// The reply is a summary of active receiver status in that format.
// Posssible formats:
// - `json`: full summary of active receiver status; an object indexed
//   by serial numbers
// - `port`: list of tunnelPorts of connected receivers, one per line
// - `serno`: list of serial numbers of connected receivers, one per line

func handleStatusConn(conn net.Conn) {
	buff := make([]byte, 4096)
	var lr = NewLineReader(conn, &buff)
	cmds := map[string]int8{
		"who":    CMD_WHO,
		"port":   CMD_PORT,
		"ports":  CMD_PORT,
		"serno":  CMD_SERNO,
		"sernos": CMD_SERNO,
		"status": CMD_JSON,
		"json":   CMD_JSON,
		"quit":   CMD_QUIT}
ConnLoop:
	for {
		err := lr.getLine()
		if err != nil {
			break ConnLoop
		}
		var b string
		cmd, ok := cmds[string(buff)]
		if !ok {
			b = "Error: command must be one of: "
			for c, _ := range cmds {
				b += "\n" + c
			}

		} else {
			switch cmd {
			case CMD_QUIT:
				break ConnLoop
			case CMD_JSON:
				bb := make([]byte, 0, 1000)
				bb = append(bb, '{')
				activeSGs.Range(func(serno interface{}, sgp interface{}) bool {
					sg := sgp.(*ActiveSG)
					sg.lock.Lock()
					js, err := json.Marshal(sg)
					sg.lock.Unlock()
					if err == nil {
						if len(bb) > 1 {
							bb = append(bb, ',')
						}
						bb = append(bb, "\""+string(serno.(Serno))+"\":"...)
						bb = append(bb, js...)
					}
					return true
				})
				bb = append(bb, '}')
				b = string(bb)
			case CMD_WHO, CMD_PORT, CMD_SERNO:
				activeSGs.Range(func(serno interface{}, sgp interface{}) bool {
					sg := sgp.(*ActiveSG)
					sg.lock.Lock()
					if sg.Connected {
						if cmd == CMD_SERNO || cmd == CMD_WHO {
							b += string(sg.Serno)
						}
						if cmd == CMD_WHO {
							b += ","
						}
						if cmd == CMD_PORT || cmd == CMD_WHO {
							b += strconv.Itoa(sg.TunnelPort)
						}
						b += "\n"
					}
					sg.lock.Unlock()
					return true
				})
			}
		}
		_, err = io.WriteString(conn, b)
		if err != nil {
			break ConnLoop
		}
	}
	conn.Close()
}

// listen for status request connections and dispatch them to a handler
func StatusServer(ctx context.Context, address string) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		print("failed to resolve address " + address)
		return
	}
	srv, err := net.ListenTCP("tcp", addr)
	if err != nil {
		print("failed to listen on " + address)
		return
	}
	defer srv.Close()
	for {
		conn, err := srv.AcceptTCP()
		if err != nil {
			// handle error
			print("problem accepting connection")
			return
		}
		go handleStatusConn(net.Conn(conn))
	}
	select {
	case <-ctx.Done():
	}
}

// check whether credentials authorize an operation on an SG
func Auth(serno Serno, op string, creds string) bool {
	parts := strings.Split(creds, ",")
	if parts[0] == "motus" && len(parts) == 3 {
		// credentials are for a motus user
		// validate them
		nows := time.Now().Format("20060102150405")
		client := &http.Client{Timeout: 30 * time.Second}
		res, err := client.Get(fmt.Sprintf(MotusAuthUser, nows, url.QueryEscape(parts[1]), url.QueryEscape(parts[2])))
		var auth APIResAuth
		dec := json.NewDecoder(res.Body)
		err = dec.Decode(&auth)
		if err != nil {
			// user authentication failed
			return false
		}
		// if SG is known at motus, user must belong to project under which it is deployed
		// otherwise, user must simply be registered with motus
		dep, known := MotusInfo.RecvDeps[serno]
		if known {
			_, inproj := auth.Projects[strconv.Itoa(dep.ProjectID)]
			if !inproj {
				return false
			}
		}
		return true
	}
	return false
}

// type representing an SG registration
type Registration struct {
	serno      Serno  // serial number
	tunnelPort int    // designated ssh tunnel port
	pubKey     string // public encryption key
	privKey    string // private encryption key
}

// Handle registration requests.
//
// The request is a one line format:
//
//    SERNO: a bare SG serial number, e.g. SG-1234BBBK5678.
// or
//    SERNO,motus,USER,PASSWORD: a serial number followed by credential type (motus) and credentials
//
// On success, the reply is three lines:
// ```
// TUNNEL_PORT
// PUB_KEY
// PRIV_KEY
// ```
// where:
//
// - `TUNNEL_PORT` is the integer port number the SG is allowed to map from the server to
//     its own sshd port
// - `PUB_KEY`, `PRIV_KEY` is the cryptographic key pair the SG can use to connect to the server to send data and to map the tunnel port
//
// For unsuccessful requests, we close the connection without replying.
//
// The request succeeds only in these cases:
//
//  - `SERNO` not seen before (tunnelPort, pubKey, privKey are then generated from scratch)
//  - `SERNO` seen before; connection from trusted IP address
//  - `SERNO` seen before; connection from untrusted IP address; valid credentials given
//
func handleRegConn(conn net.Conn) {
	buff := make([]byte, 4096)
	var lr = NewLineReader(conn, &buff)
	err := lr.getLine()
	{
		if err != nil {
			goto Done
		}
		serno := buff[0:15]
		if !SernoRegexp.Match(serno) {
			// invalid serno
			goto Done
		}
		// is this connection from a trusted IP address?
		trusted := TrustedIPAddrRegexp.MatchString(conn.RemoteAddr().String())
		// has this SG been seen before?
		var reg Registration
		known := SQL(DBQGetRegistration, c(string(serno)), c(&reg.tunnelPort, &reg.pubKey, &reg.privKey))
		if !known && RegisterSG(Serno(serno), &reg) != nil {
			goto Done
		}
		// see whether we need to authenticated request
		if known && !trusted && !Auth(Serno(serno), "register", string(buff[16:])) {
			goto Done
		}
		_, err = io.WriteString(conn, fmt.Sprintf("%d\n%s%s", reg.tunnelPort, reg.pubKey, reg.privKey))
	}
Done:
	conn.Close()
}

// create a new registration for an SG server into the existing struct
//
// return Error on failure, nil on success
func RegisterSG(serno Serno, reg *Registration) error {
	ok := SQL(DBQNewSG, c(serno), c())
	if !ok {
		// unable to create new SG record (!) out of tunnel ports?  Obvious DOS attack vector here!
		return fmt.Errorf("unable to register new SG: %s", serno)
	}
	reg = &Registration{serno: serno}
	// read back the tunnelPort just generated
	if !SQL(DBQGetRegistration, c(serno), c(&reg.tunnelPort, &reg.pubKey, &reg.privKey)) {
		return fmt.Errorf("unable to read registration record for %s", serno)
	}

	// FIXME: deal with keys having already been generated on sgdata.motus.org
	// because data files were seen before the receiver was.

	// alt_keyfile_name = ALT_KEYPATH + "id_dsa_sg_" + serno

	// if os.path.exists(alt_keyfile_name):
	//     # move the existing keys to the new location
	//     os.rename(alt_keyfile_name, keyfile_name)
	//     os.rename(alt_keyfile_name + ".pub", keyfile_name + ".pub")
	// else:
	// 	# generate a pub/priv keypair
	keyfile := path.Join(CryptoKeyPath, "id_rsa_"+string(serno))
	err := exec.Command("ssh-keygen", "-t", "rsa", "-f", keyfile, "-N", "").Run()
	if err != nil {
		return err
	}
	// export an openssl-compatible version of the public key
	// for use in signature verification
	// sample command: openssl dsa -in ~sg_remote/.ssh/id_dsa_sg_2814BBBK4765 -pubout -out ~sg_remote/.ssh/id_dsa_sg_2814BBBK4765.openssl.pub
	err = exec.Command("openssl", "rsa", "-in", keyfile, "-pubout", "-out", keyfile+".openssl.pub").Run()
	if err != nil {
		return err
	}
	privkey, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return err
	}
	pubkey, err := ioutil.ReadFile(keyfile + ".pub")
	if err != nil {
		return err
	}

	if !SQL(DBQNewSGKeys, c(unixtime(time.Now()), pubkey, privkey, true, string(serno)), c()) {
		return fmt.Errorf("Unable to record crypto keys for %s", serno)
	}

	//     auth_key_file = open(SSH_DIR + "authorized_keys", "a")
	//     # restrict this key to running sg_remote and only mapping a single remote port
	//     # local port mapping is restricted to reach port 7 on the host, which if it
	//     # is even available is the echo port.  This means even if someone gets
	//     # hold of a unique pub/priv key pair for a sensorgnome, at most they can
	//     # fill up an sqlite database with junk, and can't connect to any services
	//     # on the host.

	f, err := os.OpenFile(CryptoAuthKeysPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, `command="/bin/false",no-pty,no-X11-forwarding,single-remote-forwarding-port=%d,permitopen="localhost:%s",permitopen="localhost:7",environment="SG_SERNO=%s",environment="SG_PORT=%d",connection-semname="%s" %s`,
		reg.tunnelPort, TrustedStreamPort, string(serno), reg.tunnelPort, string(serno), pubkey)
	f.Close()
	return err
}

// listen for SG registration request connections and dispatch them to a handler
func RegistrationServer(ctx context.Context, address string) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		print("failed to resolve address " + address)
		return
	}
	srv, err := net.ListenTCP("tcp", addr)
	if err != nil {
		print("failed to listen on " + address)
		return
	}
	defer srv.Close()
	for {
		conn, err := srv.AcceptTCP()
		if err != nil {
			// handle error
			print("problem accepting connection")
			return
		}
		go handleRegConn(net.Conn(conn))
	}
	select {
	case <-ctx.Done():
	}
}

// regenerate the main status page when StatusChange messages are received
//
// `latency` is the maximum time to wait before regnerating a page given
// a StatusChange message has arrived, but also the minimum time between
// regenerations.
//
// `motusLatency` is the maximum time to wait before regenerating the motus
// metadata, which gives receiver deployment name and project.
func StatusPageMaintainer(path string, latency time.Duration) {
	evt := Bus.Sub(MsgStatusChange)
	go func() {
		defer evt.Unsub("*")
		var regen, latent bool
		wait := time.NewTimer(latency)
	MsgLoop:
		for {
			regen, latent = false, false
			for !(regen && latent) {
				// wait for StatusChange messages and/or the latency timer
				select {
				case _, ok := <-evt.Msgs():
					if !ok {
						break MsgLoop
					}
					regen = true
				case _ = <-wait.C:
					latent = true
				}
			}
			UpdateMotusCache()
			MakeStatusPage(path)
			wait.Reset(latency)
		}
		wait.Stop()
	}()
}

func mkTime(t time.Time) string {
	if t.IsZero() {
		return "?"
	}
	return t.Format(ShortTimestampFormat)
}

func MakeStatusPage(path string) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "## Networked SensorGnomes ##")

	fmt.Fprintln(f, mkTime(time.Now()))
	fmt.Fprintln(f, "{{< bootstrap-table \"table table-striped table-bordered\" >}}")
	fmt.Fprintln(f, "\nSerial Number (Port)|Site (Project)|Connected?|Last Con. / Discon.|motus.org Last Sync|motus.org Next Sync"+
		"\n:------------------:|--------------|:--------:|-----------------|-------------------|-------------------")

	var lines []string
	activeSGs.Range(func(serno interface{}, sgp interface{}) bool {
		sg := sgp.(*ActiveSG)
		rdep := MotusInfo.RecvDeps[serno.(Serno)]
		var status string
		var tcon time.Time
		if sg.Connected {
			status = "Yes"
			tcon = sg.TsConn
		} else {
			status = "No"
			tcon = sg.TsDisConn
		}
		line := fmt.Sprintf(`%s (%d)|%s (%s)|%s|%s|<a href="https://sgdata.motus.org/status?jobsForSerno=%s&excludeSync=0" target="_blank">%s</a>|%s`, serno.(Serno), sg.TunnelPort, rdep.SiteName, MotusInfo.Projects[rdep.ProjectID], status, mkTime(tcon), serno.(Serno), mkTime(sg.TsLastSync), mkTime(sg.TsNextSync))
		lines = append(lines, line)
		return true
	})
	sort.Strings(lines)
	fmt.Fprintln(f, strings.Join(lines, "\n"))
	fmt.Fprintf(f, "{{< /bootstrap-table >}}\n")
}

type RecvDep struct {
	ProjectID int
	SiteName  string
}

type MotusCache struct {
	lastFetch time.Time // time motus data last fetched
	Latency   time.Duration
	Projects  map[int]string    // project names by id
	RecvDeps  map[Serno]RecvDep // receiver deployments by Serno
}

var MotusInfo *MotusCache

type APIResProj struct {
	Data []struct {
		Id   int
		Code string
	}
}

type APIResRecv struct {
	Data []struct {
		ReceiverID     string
		DeploymentName string
		RecvProjectID  int
	}
}

type APIResAuth struct {
	UserID       int
	EmailAddress string
	Projects     map[string]string
}

// maintain the motus metadata cache
func UpdateMotusCache() {
	if MotusInfo == nil {
		MotusInfo = &MotusCache{Latency: MotusMinLatency * time.Minute, Projects: make(map[int]string), RecvDeps: make(map[Serno]RecvDep)}
	}
	now := time.Now()
	if now.Sub(MotusInfo.lastFetch) > MotusInfo.Latency {
		client := &http.Client{Timeout: 30 * time.Second}
		nows := now.Format("20060102150405")
		res, err := client.Get(fmt.Sprintf(MotusGetProjectsUrlT, nows))
		got := 0
		if err == nil {
			var projs APIResProj
			dec := json.NewDecoder(res.Body)
			err = dec.Decode(&projs)
			for _, x := range projs.Data {
				MotusInfo.Projects[x.Id] = x.Code
			}
			got++
		}
		res, err = client.Get(fmt.Sprintf(MotusGetReceiversUrlT, nows))
		if err == nil {
			var recvs APIResRecv
			dec := json.NewDecoder(res.Body)
			err = dec.Decode(&recvs)
			for _, x := range recvs.Data {
				MotusInfo.RecvDeps[Serno(x.ReceiverID)] = RecvDep{ProjectID: x.RecvProjectID, SiteName: x.DeploymentName}
			}
			got++
		}
		if got == 2 {
			MotusInfo.lastFetch = now
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	Bus = mbus.NewMbus()
	var ctx, _ = context.WithCancel(context.Background())
	DB = OpenDB(SGDBFile)

	//
	//         Message Consumers
	//
	// Each consumer does two things:
	//   - subscribe to messages; happens immediately in the main goroutine
	//   - handle subscribed messages; happens in a new goroutine launched by the consumer
	// So as soon as the consumer's main function returns, it is ready to
	// receive messages on topics it has subscribed to. i.e. no messages will be missed,
	// even if the new goroutine has not run yet.

	// record messages to a database
	DBRecorder()

	// maintain the list of active SGs
	SGMinder()

	// manage sync jobs on attached SGs
	SyncManager()

	// messageDump() // DEBUG

	// maintain an up-to-date status page, but don't update more than
	// once every 5 seconds and don't wait longer than that to update
	// when needed.
	StatusPageMaintainer(StatusPagePath, StatusPageMinLatency*time.Second)

	//
	//         Message Producers
	//
	// Producers are launched after consumers have subscribed
	// to message topics.

	ConnectionWatcher(ctx, ConnectionSemPath, ConnectionSemRE)
	go StatusServer(ctx, AddressStatusServer)
	go TrustedStreamSource(ctx, AddressTrustedStream)
	go DgramSource(ctx, AddressUntrustedDgram, false)
	go DgramSource(ctx, AddressTrustedDgram, true)
	go RegistrationServer(ctx, AddressRegServer)
	// wait until cancelled (nothing does this, though)
	<-ctx.Done()
}
