package utp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/anacrolix/missinggo"
	"github.com/anacrolix/missinggo/inproc"
	"github.com/bradfitz/iter"

	"github.com/go-quicktest/qt"
)

func TestAcceptOnDestroyedSocket(t *testing.T) {
	pc, err := net.ListenPacket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	s, err := NewSocketFromPacketConn(pc)
	qt.Assert(t, qt.IsNil(err))
	go pc.Close()
	_, err = s.Accept()
	qt.Assert(t, qt.IsNotNil(err))
	t.Log(err.Error())
}

func TestSocketDeadlines(t *testing.T) {
	s, err := NewSocket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	defer s.Close()
	qt.Check(t, qt.IsNil(s.SetReadDeadline(time.Now())))
	_, _, err = s.ReadFrom(nil)
	qt.Check(t, qt.Equals[error](err, errTimeout))
	qt.Check(t, qt.IsNil(s.SetWriteDeadline(time.Now())))
	_, err = s.WriteTo(nil, nil)
	qt.Check(t, qt.Equals[error](err, errTimeout))
	qt.Check(t, qt.IsNil(s.SetDeadline(time.Time{})))
	qt.Check(t, qt.IsNil(s.Close()))
}

func TestSaturateSocketConnIDs(t *testing.T) {
	s, err := NewSocket("inproc", "")
	qt.Assert(t, qt.IsNil(err))
	defer s.Close()
	var acceptedConns, dialedConns []net.Conn
	for range iter.N(500) {
		accepted := make(chan struct{})
		go func() {
			c, err := s.Accept()
			if err != nil {
				t.Log(err)
				return
			}
			acceptedConns = append(acceptedConns, c)
			close(accepted)
		}()
		c, err := s.Dial(s.Addr().String())
		qt.Assert(t, qt.IsNil(err))
		dialedConns = append(dialedConns, c)
		<-accepted
	}
	t.Logf("%d dialed conns, %d accepted", len(dialedConns), len(acceptedConns))
	for i := range iter.N(len(dialedConns)) {
		data := []byte(fmt.Sprintf("%7d", i))
		dc := dialedConns[i]
		n, err := dc.Write(data)
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.Equals(n, 7))
		qt.Assert(t, qt.IsNil(dc.Close()))
		var b [8]byte
		ac := acceptedConns[i]
		n, err = ac.Read(b[:])
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.Equals(n, 7))
		qt.Assert(t, qt.DeepEquals(b[:n], data))
		n, err = ac.Read(b[:])
		qt.Assert(t, qt.Equals(n, 0))
		qt.Assert(t, qt.Equals(err, io.EOF))
		ac.Close()
	}
}

func TestUTPRawConn(t *testing.T) {
	l, err := NewSocket("inproc", "")
	qt.Assert(t, qt.IsNil(err))
	defer l.Close()
	go func() {
		for {
			_, err := l.Accept()
			if err != nil {
				break
			}
		}
	}()
	// Connect a UTP peer to see if the RawConn will still work.
	utpPeer := func() net.Conn {
		s, _ := NewSocket("inproc", "")
		defer s.Close()
		ret, err := s.Dial(fmt.Sprintf("localhost:%d", missinggo.AddrPort(l.Addr())))
		qt.Assert(t, qt.IsNil(err))
		return ret
	}()
	if err != nil {
		t.Fatalf("error dialing utp listener: %s", err)
	}
	defer utpPeer.Close()
	peer, err := inproc.ListenPacket("inproc", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	msgsReceived := 0
	const N = 500 // How many messages to send.
	readerStopped := make(chan struct{})
	// The reader goroutine.
	go func() {
		defer close(readerStopped)
		b := make([]byte, 500)
		for i := 0; i < N; i++ {
			n, _, err := l.ReadFrom(b)
			if err != nil {
				t.Fatalf("error reading from raw conn: %s", err)
			}
			msgsReceived++
			var d int
			fmt.Sscan(string(b[:n]), &d)
			if d != i {
				log.Printf("got wrong number: expected %d, got %d", i, d)
			}
		}
	}()
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", missinggo.AddrPort(l.Addr())))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		_, err := peer.WriteTo([]byte(fmt.Sprintf("%d", i)), udpAddr)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
	}
	select {
	case <-readerStopped:
	case <-time.After(time.Second):
		t.Fatal("reader timed out")
	}
	if msgsReceived != N {
		t.Fatalf("messages received: %d", msgsReceived)
	}
}

func TestAcceptGone(t *testing.T) {
	s, err := NewSocket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = DialContext(ctx, s.Addr().String())
	qt.Assert(t, qt.IsNotNil(err))
	// Will succeed because we don't signal that we give up dialing, or check
	// that the handshake is completed before returning the new Conn.
	c, err := s.Accept()
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()
	err = c.SetReadDeadline(time.Now().Add(time.Millisecond))
	qt.Assert(t, qt.IsNil(err))
	_, err = c.Read(nil)
	qt.Assert(t, qt.ErrorMatches(err, regexp.QuoteMeta("i/o timeout")))
}
