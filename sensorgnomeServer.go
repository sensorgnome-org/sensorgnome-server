package main

import (
	"fmt"
	"io"
	"net"
	"log"
	"time"
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
//	"github.com/fsnotify/fsnotify"

)

// The message type; `sender` is the authenticated origin of `text`
type Message struct {
	sender string // typically the SG serial number
	text   string // typically a JSON-formatted message
}

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

// A sink for Messages which dumps them to stdout.
func messageDump(src <-chan Message) {
	for m := range src {
		fmt.Printf("%s: %s\n", m.sender, m.text)
	}
}

type Sink func (<-chan Message)

// Create a sink for Messages which stores them in an sqlite table.
// This table must have (at least) fields "sender", "ts" and "message"
func NewSqliteSink(ctx context.Context, src<-chan Message, dbfile string, table string) Sink {
	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		log.Fatal(err)
	}
	sqlStmt := fmt.Sprintf(`
                CREATE TABLE IF NOT EXISTS %s (
                    ts DOUBLE PRIMARY KEY,
                    sender TEXT,
                    message TEXT
                )` , table)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil
	}

	sqlStmt = fmt.Sprintf(`
                CREATE INDEX IF NOT EXISTS %s_sender ON %s(sender)`,
		table, table)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil
	}
	sqlStmt = fmt.Sprintf("SELECT ts, sender, message FROM %s LIMIT 0", table)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil
	}

	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s(ts, sender, message) values (?, ?, ?)", table))
	if err != nil {
		log.Fatal(err)
	}
	// create closure that uses stmt, db
	return func (src <-chan Message) {
		for {
			select {
			case m:= <-src:
				t := time.Now()
				_, err := stmt.Exec(float64(t.UnixNano()) / 1.0E9, m.sender, m.text)
				if err != nil {
					log.Fatal(err)
				}
			case <-ctx.Done():
				stmt.Close()
				db.Close()
				return
			}
		}
	}
}

func main() {
	var ctx, _ = context.WithCancel(context.Background())
	var msg = make(chan Message)
	var sql = NewSqliteSink(ctx, msg, "/home/sg_remote/sg_remote.sqlite", "messages")
	go trustedStreamSource(ctx, "localhost:59024", msg)
	go dgramSource(ctx, ":59022", false, msg)
	go dgramSource(ctx, ":59023", true, msg)
	go sql(msg)
//	go messageDump(msg)
	<-ctx.Done()
}
