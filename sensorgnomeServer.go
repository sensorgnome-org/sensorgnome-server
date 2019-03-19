package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/fsnotify/fsnotify"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	//	"os"
	"os/exec"
	//	"path"
	"regexp"
	"strconv"
	"time"
)

// customization constants
const (
	MotusUser         = "sg@sgdata.motus.org"                          // user on sgdata.motus.org; this is who ssh makes us be
	MotusUserKey      = "/home/sg_remote/.ssh/id_ed25519_sgorg_sgdata" // ssh key to use for sync on sgdata.motus.org
	MotusControlPath  = "/home/sg_remote/sgdata.ssh"                   // control path for multiplexing port mappings to sgdata.motus.org
	SyncWaitLo        = 30                                             // minimum time between syncs of a receiver (minutes)
	SyncWaitHi        = 90                                             // maximum time between syncs of a receiver (minutes)
	SyncTimeDir       = "/home/sg_remote/last_sync"                    // directory with one file per SG; mtime is last sync time
	SGDBFile          = "/home/sg_remote/sg_remote.sqlite"             // sqlite database with receiver info
	ConnectionSemPath = "/dev/shm"                                     // directory where sshd maintains semaphores indicating connected SGs
	ConnectionSemRE   = "sem.(SG-[0-9A-Z]{12})"                        // regular expression for matching SG semaphores (capture group is serno)
)

// The message type; `sender` is the authenticated origin of `text`
type Message struct {
	ts     float64 // timestamp; if 0, means not set
	sender string  // typically the SG serial number
	text   string  // typically a JSON- or CSV- formatted message
}

// type representing an SG serial number
type Serno string

// an SG we have seen recently
type ActiveSG struct {
	serno      Serno     // serial number; e.g. "SG-1234BBBK9812"
	tsConn     time.Time // time at which connected
	tsLastSync time.Time // time at which last synced with motus
	tsNextSync time.Time // time at which next to be synced with motus
	tunnelPort int       // ssh tunnel port, if applicable
	connected  bool      // actually connected?  once we've seen a receiver, we keep this struct in memory, but set this field to false when it disconnects
}

// types of SG events
type SGEventType int

const (
	SGDisconnect SGEventType = iota // connected via ssh
	SGConnect                       // disconnected from ssh
	SGSync                          // data sync with motus.org
)

// event regarding an SG
type SGEvent struct {
	serno Serno       // serial number
	ts    time.Time   // time of event
	kind  SGEventType // what kind of event
}

// pipe for SG events
type SGEventSink chan<- SGEvent

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
// provides the sender, which is used in the Messages generated
// from all subsequent lines.
func handleTrustedStream(conn net.Conn, dst chan<- Message) {
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
		dst <- Message{sender: sender, text: string(buff)}
	}
}

// listen for trusted streams and dispatch them to a handler
func trustedStreamSource(ctx context.Context, address string, dst chan<- Message) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		print("failed to resolve address localhost:59024")
		return
	}
	srv, err := net.ListenTCP("tcp", addr)
	if err != nil {
		print("failed to listen on port 59024")
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
		go handleTrustedStream(net.Conn(conn), dst)
	}
	select {
	case <-ctx.Done():
	}
}

// Listen for datagrams on either a trusted or untrusted port.
// Datagrams from the trusted port are treated as authenticated.
// Datagrams from an untrusted port have their signature checked
// and are discarded if this is not valid.
// Datagrams are passed to the dst channel as Messages.
func dgramSource(ctx context.Context, address string, trusted bool, dst chan<- Message) {
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

// Debug: A sink for Messages which dumps them to stdout.
func messageDump(src <-chan Message) {
	for m := range src {
		fmt.Printf("%s: %s\n", m.sender, m.text)
	}
}

// Goroutine that accepts Messages and stores them in an sqlite
// table called "messages" in the global DB.
func sqliteSink(ctx context.Context, src <-chan Message) {
	stmt, err := DB.Prepare("INSERT INTO messages (ts, sender, message) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	// create closure that uses stmt, db
	defer stmt.Close()
	for {
		select {
		case m := <-src:
			if m.ts == 0 {
				m.ts = float64(time.Now().UnixNano()) / 1.0E9
			}
			_, err := stmt.Exec(m.ts, m.sender, m.text)
			if err != nil {
				log.Fatal(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// get the latest sync time for a serial number
// uses global `DB`; returns 0 if no sync has occurred
func SGSyncTime(serno Serno) (lts time.Time) {
	sqlStmt := fmt.Sprintf(`
                   SELECT max(ts) FROM messages WHERE sender = '%s' and substr(message, 1, 1) == '2'`,
		string(serno))
	rows, err := DB.Query(sqlStmt)
	defer rows.Close()
	if err == nil {
		if rows.Next() {
			var ts float64
			rows.Scan(&ts)
			lts = time.Unix(0, int64(ts*1E9))
		}
	}
	return
}

// get the tunnel port for a serial number
// uses global `DB`; returns 0 on error
func TunnelPort(serno Serno) (t int) {
	sqlStmt := fmt.Sprintf(`
                   SELECT tunnelPort FROM receivers WHERE serno='%s'`,
		string(serno))
	rows, err := DB.Query(sqlStmt)
	defer rows.Close()
	if err == nil {
		if rows.Next() {
			rows.Scan(&t)
		}
	}
	return
}

// map from serial number to boolean indicating whether an
// SG is connected

var activeSGs map[Serno]*ActiveSG

// watch directory `dir` for creation / deletion of files
// representing connected SGs, and pass appropriate events
// to `sink`. Files representing SGs are those matching
// the first capture group of `sernoRE`.
// After establishing a watch goroutine, events are generated for
// any files already in `dir`, using the file mtime.  This creates
// a race condition under which `sink` might see two SGConnect events
// for the same SG.

func ConnectionWatcher(ctx context.Context, dir string, sernoRE string, sink SGEventSink) {
	activeSGs = make(map[Serno]*ActiveSG)
	re := regexp.MustCompile(sernoRE)
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
					serno := Serno(parts[1])
					now := time.Now()
					if event.Op&fsnotify.Create == fsnotify.Create {
						if _, ok = activeSGs[serno]; !ok {
							// an SG we haven't seen before during this server session
							activeSGs[serno] = &ActiveSG{serno: serno, tsConn: now, tsLastSync: SGSyncTime(serno), tunnelPort: TunnelPort(serno), connected: true}
						} else {
							// SG already on active list, so just update connection time and status
							activeSGs[serno].tsConn = now
							activeSGs[serno].connected = true
						}
						sink <- SGEvent{serno, now, SGConnect}
					} else if event.Op&fsnotify.Remove == fsnotify.Remove {
						activeSGs[serno].connected = false
						sink <- SGEvent{serno, now, SGDisconnect}
					}
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
	files, err := ioutil.ReadDir(dir)
	if err == nil {
		for _, finfo := range files {
			parts := re.FindStringSubmatch(finfo.Name())
			if parts != nil {
				serno := Serno(parts[1])
				activeSGs[serno] = &ActiveSG{serno: serno, tsConn: finfo.ModTime(), tsLastSync: SGSyncTime(serno), tunnelPort: TunnelPort(serno), connected: true}
				sink <- SGEvent{Serno(parts[1]), finfo.ModTime(), SGConnect}
			}
		}
	}
}

// used only for debugging
func ConnectionLogger(ctx context.Context, evt <-chan SGEvent) {
	for {
		select {
		case e := <-evt:
			switch e.kind {
			case SGConnect:
				fmt.Printf("Connect: %s\n", e.serno)
			case SGDisconnect:
				fmt.Printf("Disconnect: %s\n", e.serno)
			}
		case <-ctx.Done():
			return
		}
	}
}

// manage repeated sync jobs for a single SG
// emit a message each time a receiver sync is launched
func SyncWorker(ctx context.Context, serno Serno, msg chan<- Message) {
	// grab receiver info pointer
	sg, ok := activeSGs[serno]
	if !ok {
		return
	}
	cp := fmt.Sprintf("-oControlPath=%s", MotusControlPath)
	pf := fmt.Sprintf("-R%d:localhost:%d", sg.tunnelPort, sg.tunnelPort)
	tf := fmt.Sprintf("/sgm_local/sync/method=%d,serno=%s", sg.tunnelPort, string(serno))

	for {
		// set up a wait uniformly distributed between 30 and 90 minutes
		delay := time.Duration(1E9 * (60 * (SyncWaitLo + rand.Intn(SyncWaitHi-SyncWaitLo))))
		wait := time.NewTimer(delay)
		sg.tsNextSync = time.Now().Add(delay)
		select {
		case synctime := <-wait.C:
			// if receiver is not still connected, end this goroutine
			if !activeSGs[serno].connected {
				return
			}
			cmd := exec.Command("ssh", "-i", MotusUserKey, "-f", "-N", "-T",
				"-oStrictHostKeyChecking=no", "-oExitOnForwardFailure=yes", "-oControlMaster=auto",
				"-oServerAliveInterval=5", "-oServerAliveCountMax=3",
				cp, pf, MotusUser)
			err := cmd.Run()
			if err != nil {
				fmt.Println(err.Error())
			}
			// ignoring error; it is likely just the failure to map an already mapped port
			cmd = exec.Command("ssh", "-i", MotusUserKey, "-oControlMaster=auto",
				cp, MotusUser, "touch", tf)
			err = cmd.Run()
			if err == nil {
				activeSGs[serno].tsLastSync = synctime
				msg <- Message{sender: string(serno), text: strconv.Itoa(int(SGSync))}
			} else {
				fmt.Println(err.Error())
			}

		case <-ctx.Done():
			wait.Stop()
			return
		}
	}
}

// manage events for SGs
// When evt is `SGConnect`, start a goroutine that periodically
// launches a motus sync job.  When evt is `SGDisconnect`, stop
// the associated goroutine.  Multiple `SGConnect` events for
// the same receiver are collapsed into the first one.  Events
// are recorded as messages.
func EventManager(ctx context.Context, evt <-chan SGEvent, msg chan<- Message) {
	syncCancels := make(map[Serno]context.CancelFunc)
	for {
		select {
		case event, ok := <-evt:
			if ok {
				serno := event.serno
				msg <- Message{sender: string(serno), ts: float64(event.ts.UnixNano()) / 1.0E9, text: strconv.Itoa(int(event.kind))}
				_, have := syncCancels[serno]
				switch event.kind {
				case SGConnect:
					if have {
						break
					}
					newctx, cf := context.WithCancel(ctx)
					syncCancels[serno] = cf
					go SyncWorker(newctx, serno, msg)
				case SGDisconnect:
					if !have {
						break
					}
					syncCancels[serno]()
					delete(syncCancels, serno)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// global database pointer
var DB *sql.DB

// open/create the main database
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
	return
}

func main() {
	rand.Seed(time.Now().UnixNano())
	var ctx, _ = context.WithCancel(context.Background())
	DB = OpenDB(SGDBFile)
	var msg = make(chan Message)
	evtChan := make(chan SGEvent)
	go sqliteSink(ctx, msg)
	go trustedStreamSource(ctx, "localhost:59024", msg)
	go dgramSource(ctx, ":59022", false, msg)
	go dgramSource(ctx, ":59023", true, msg)
	//	go messageDump(msg)
	go EventManager(ctx, evtChan, msg)
	ConnectionWatcher(ctx, ConnectionSemPath, ConnectionSemRE, evtChan)

	// wait until cancelled (nothing does this, though)
	<-ctx.Done()
}
