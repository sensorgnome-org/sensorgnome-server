### First attempt at a simple go server for the sensorgnome project


```go sensorgnomeServer.go
package main
import (
	"net"
	"fmt"
	"time"
	"context"
)

func handleTrustedStream(conn *net.TCPConn) {
	buff := make([]byte, 4096)
	var addr = conn.RemoteAddr()
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Printf("connection from %s closed\n", addr)
			return
		}
		fmt.Printf("Got %d bytes: %s from %s\n", n, buff, addr)
	}
}

func trustedStreamSource (ctx context.Context, address string) {
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
		go handleTrustedStream(conn)
	}
	select {
	case <-ctx.Done():
	}
}

func dgramSource (ctx context.Context, address string, trusted bool) {
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

func main() {
	var ctx, _ = context.WithCancel(context.Background())
	go trustedStreamSource(ctx, "localhost:59024")
	go dgramSource(ctx, ":59022", false)
	go dgramSource(ctx, ":59023", true)
	for {
		time.Sleep(10 * time.Second)
	}
}
```
