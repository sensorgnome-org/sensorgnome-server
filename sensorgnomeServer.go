package main
import (
	"io"
	"net"
	"fmt"
//	"time"
	"context"
//	"database/sql"
//	"github.com/mattn/go-sqlite3"
)

// The message type; `sender` is the authenticated origin of `text`
type Message struct {
	sender string // typically the SG serial number
	text string   // typically a JSON-formatted message
}


// read one line at a time from an io.Reader
type LineReader struct {
	dest *[]byte // where a single line is written
	buf []byte   // buffer for reading
	bufp int // position of next character in buf to use
	buflen int // number of characters left in buffer
	rdr io.Reader // connection being read from
}

// constructor
func NewLineReader(rdr net.Conn, dest *[]byte) *LineReader {
	r := new(LineReader)
	r.rdr = rdr
	r.dest = dest
	r.buf = make ([]byte, len(*dest))
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
			if err != nil {
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

// handle messages from a trusted stream
func handleTrustedStream(conn net.Conn, dst chan<- Message) {
	buff := make([]byte, 4096)
	var addr = conn.RemoteAddr()
	var lr = NewLineReader(conn, &buff)
	_ = lr.getLine()
	var sender string = string(buff)
	for {
		err := lr.getLine()
		if err != nil {
			fmt.Printf("connection from %s closed\n", addr)
			return
		}
		dst<- Message{sender, string(buff)}
	}
}

func trustedStreamSource (ctx context.Context, address string, dst chan<- Message ) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		print ("failed to resolve address localhost:59024")
		return
	}
	srv, err := net.ListenTCP ("tcp", addr)
	if err != nil {
		print ("failed to listen on port 59024")
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

func dgramSource (ctx context.Context, address string, trusted bool, dst chan<- Message) {
	pc, err  := net.ListenPacket ("udp", address)
	if err != nil {
		print ("failed to listen on port " + address)
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
	} ()
	select {
	case <-ctx.Done():
		fmt.Println("cancelled")
		err = ctx.Err()
	case err = <-doneChan:
	}
}

func messageDump (dst <-chan Message) {
	for m := range dst {
		fmt.Printf("%s: %s\n", m.sender, m.text)
	}
}

func main() {
	var ctx, _ = context.WithCancel(context.Background())
	var dest = make(chan Message)
	go trustedStreamSource(ctx, "localhost:59024", dest)
	go dgramSource(ctx, ":59022", false, dest)
	go dgramSource(ctx, ":59023", true, dest)
	go messageDump(dest)
	<-ctx.Done()
}
