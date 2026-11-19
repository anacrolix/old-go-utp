package utp

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/missinggo/leaktest"
	"github.com/bradfitz/iter"
	"golang.org/x/net/nettest"

	"github.com/go-quicktest/qt"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
}

func setDefaultTestingDurations() {
	writeTimeout = 1 * time.Second
	initialLatency = 10 * time.Millisecond
	packetReadTimeout = 2 * time.Second
}

func TestUTPPingPong(t *testing.T) {
	defer leaktest.GoroutineLeakCheck(t)()
	s, err := NewSocket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	defer s.Close()
	pingerClosed := make(chan struct{})
	go func() {
		defer close(pingerClosed)
		b, err := Dial(s.Addr().String())
		qt.Assert(t, qt.IsNil(err))
		defer b.Close()
		n, err := b.Write([]byte("ping"))
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.Equals(n, 4))
		buf := make([]byte, 4)
		b.Read(buf)
		qt.Assert(t, qt.Equals(string(buf), "pong"))
	}()
	a, err := s.Accept()
	qt.Assert(t, qt.IsNil(err))
	defer a.Close()
	buf := make([]byte, 42)
	n, err := a.Read(buf)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(buf[:n]), "ping"))
	n, err = a.Write([]byte("pong"))
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(n, 4))
	<-pingerClosed
}

func TestDialTimeout(t *testing.T) {
	defer leaktest.GoroutineLeakCheck(t)()
	s, _ := NewSocket("udp", "localhost:0")
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := DialContext(ctx, s.Addr().String())
	qt.Assert(t, qt.Equals(err, context.DeadlineExceeded))
}

func TestListen(t *testing.T) {
	defer leaktest.GoroutineLeakCheck(t)()
	ln, err := NewSocket("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
}

func TestMinMaxHeaderType(t *testing.T) {
	qt.Assert(t, qt.Equals(stMax, stSyn))
}

func TestCongestionWindowFollowsDelay(t *testing.T) {
	c := Conn{
		congestionWindow: 2 * maxPayloadSize,
		lastTimeDiff:     10_000,
	}
	c.updateCongestionWindow(maxPayloadSize)
	require.Equal(t, uint32(2*maxPayloadSize), c.congestionWindow)

	c.lastTimeDiff = 20_000
	c.updateCongestionWindow(maxPayloadSize)
	require.Greater(t, c.congestionWindow, uint32(2*maxPayloadSize))

	c.lastTimeDiff = 220_000
	c.updateCongestionWindow(maxPayloadSize)
	require.Less(t, c.congestionWindow, uint32(2*maxPayloadSize))
	require.GreaterOrEqual(t, c.congestionWindow, uint32(maxPayloadSize))
}

func TestConnReadDeadline(t *testing.T) {
	t.Parallel()
	ls, _ := NewSocket("udp", "localhost:0")
	defer ls.Close()
	ds, _ := NewSocket("udp", "localhost:0")
	defer ds.Close()
	dcReadErr := make(chan error)
	go func() {
		c, _ := ds.Dial(ls.Addr().String())
		defer c.Close()
		_, err := c.Read(nil)
		dcReadErr <- err
	}()
	c, _ := ls.Accept()
	dl := time.Now().Add(time.Millisecond)
	c.SetReadDeadline(dl)
	_, err := c.Read(nil)
	qt.Assert(t, qt.Equals[error](err, errTimeout))
	// The deadline has passed.
	if time.Now().Before(dl) {
		t.Fatal("deadline hasn't passed")
	}
	// Returns timeout on subsequent read.
	_, err = c.Read(nil)
	qt.Assert(t, qt.Equals[error](err, errTimeout))
	// Disable the deadline.
	c.SetReadDeadline(time.Time{})
	readReturned := make(chan struct{})
	go func() {
		c.Read(nil)
		close(readReturned)
	}()
	select {
	case <-readReturned:
		// Read returned but shouldn't have.
		t.Fatal("read returned")
	case <-time.After(time.Millisecond):
	}
	c.Close()
	if err := <-dcReadErr; err != io.EOF {
		t.Fatalf("dial conn read returned %s", err)
	}
	select {
	case <-readReturned:
	case <-time.After(time.Millisecond):
		t.Fatal("read should return after Conn is closed")
	}
}

func connectSelfLots(n int, t testing.TB) {
	defer leaktest.GoroutineLeakCheck(t)()
	s, err := NewSocket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	go func() {
		for range iter.N(n) {
			c, err := s.Accept()
			if err != nil {
				log.Fatal(err)
			}
			defer c.Close()
		}
	}()
	dialErr := make(chan error)
	connCh := make(chan net.Conn)
	dialSema := make(chan struct{}, backlog)
	for range iter.N(n) {
		go func() {
			dialSema <- struct{}{}
			c, err := s.Dial(s.Addr().String())
			<-dialSema
			if err != nil {
				dialErr <- err
				return
			}
			connCh <- c
		}()
	}
	conns := make([]net.Conn, 0, n)
	for range iter.N(n) {
		select {
		case c := <-connCh:
			conns = append(conns, c)
		case err := <-dialErr:
			t.Fatal(err)
		}
	}
	for _, c := range conns {
		if c != nil {
			c.Close()
		}
	}
	sleepWhile(&mu, func() bool { return len(s.conns) != 0 })
	s.Close()
}

// Connect to ourself heaps.
func TestConnectSelf(t *testing.T) {
	// A rough guess says that at worst, I can only have 0x10000/3 connections
	// to the same socket, due to fragmentation in the assigned connection
	// IDs.
	connectSelfLots(0x100, t)
}

func BenchmarkConnectSelf(b *testing.B) {
	for range iter.N(b.N) {
		connectSelfLots(2, b)
	}
}

func BenchmarkNewCloseSocket(b *testing.B) {
	for range iter.N(b.N) {
		s, err := NewSocket("udp", "localhost:0")
		if err != nil {
			b.Fatal(err)
		}
		err = s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRejectDialBacklogFilled(t *testing.T) {
	s, err := NewSocket("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	errChan := make(chan error)
	dial := func() {
		_, err := s.Dial(s.Addr().String())
		qt.Assert(t, qt.IsNotNil(err))
		errChan <- err
	}
	// Fill the backlog.
	for range iter.N(backlog) {
		go dial()
	}
	sleepWhile(&mu, func() bool { return len(s.backlog) < backlog })
	select {
	case err := <-errChan:
		t.Fatalf("got premature error: %s", err)
	default:
	}
	// One more connection should cause a dial attempt to get reset.
	go dial()
	err = <-errChan
	qt.Check(t, qt.ErrorMatches(err, regexp.QuoteMeta("peer reset")))
	s.Close()
	for range iter.N(backlog) {
		<-errChan
	}
}

// Make sure that we can reset AfterFunc timers, so we don't have to create
// brand new ones everytime they fire. Specifically for the Conn resend timer.
func TestResetAfterFuncTimer(t *testing.T) {
	t.Parallel()
	fired := make(chan struct{})
	timer := time.AfterFunc(time.Millisecond, func() {
		fired <- struct{}{}
	})
	<-fired
	if timer.Reset(time.Millisecond) {
		// The timer should have expired
		t.FailNow()
	}
	<-fired
}

func connPairSocket(s *Socket) (initer, accepted net.Conn) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		initer, err = s.Dial(s.Addr().String())
		if err != nil {
			panic(err)
		}
	}()
	accepted, err := s.Accept()
	if err != nil {
		panic(err)
	}
	wg.Wait()
	return
}

func connPair() (initer, accepted net.Conn) {
	s, err := NewSocket("inproc", ":0")
	if err != nil {
		panic(err)
	}
	defer s.Close()
	return connPairSocket(s)
}

// Check that peer sending FIN doesn't cause unread data to be dropped in a
// receiver.
func TestReadFinishedConn(t *testing.T) {
	a, b := connPair()
	defer a.Close()
	defer b.Close()
	mu.Lock()
	originalAPDC := artificialPacketDropChance
	artificialPacketDropChance = 1
	mu.Unlock()
	n, err := a.Write([]byte("hello"))
	qt.Assert(t, qt.Equals(n, 5))
	qt.Assert(t, qt.IsNil(err))
	n, err = a.Write([]byte("world"))
	qt.Assert(t, qt.Equals(n, 5))
	qt.Assert(t, qt.IsNil(err))
	mu.Lock()
	artificialPacketDropChance = originalAPDC
	mu.Unlock()
	a.Close()
	all, err := ioutil.ReadAll(b)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(all), "helloworld"))
}

func TestCloseDetachesQuickly(t *testing.T) {
	t.Parallel()
	s, _ := NewSocket("udp", "localhost:0")
	defer s.Close()
	go func() {
		a, _ := s.Dial(s.Addr().String())
		a.Close()
	}()
	b, _ := s.Accept()
	b.Close()
	sleepWhile(&mu, func() bool { return len(s.conns) != 0 })
}

// Check that closing, and resulting detach of a Conn doesn't close the parent
// Socket. We Accept, then close the connection and ensure it's detached. Then
// Accept again to check the Socket is still functional and unclosed.
func TestConnCloseUnclosedSocket(t *testing.T) {
	t.Parallel()
	s, err := NewSocket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	defer func() {
		qt.Check(t, qt.IsNil(s.Close()))
	}()
	// Prevents the dialing goroutine from closing its end of the Conn before
	// we can check that it has been registered in the listener.
	dialerSync := make(chan struct{})

	go func() {
		for range iter.N(2) {
			c, err := Dial(s.Addr().String())
			qt.Assert(t, qt.IsNil(err))
			<-dialerSync
			err = c.Close()
			qt.Assert(t, qt.IsNil(err))
		}
	}()
	for range iter.N(2) {
		a, err := s.Accept()
		qt.Assert(t, qt.IsNil(err))
		// We do this in a closure because we need to unlock Server.mu if the
		// test failure exception is thrown. "Do as we say, not as we do" -Go
		// team.
		func() {
			mu.Lock()
			defer mu.Unlock()
			qt.Assert(t, qt.HasLen(s.conns, 1))
		}()
		dialerSync <- struct{}{}
		qt.Assert(t, qt.IsNil(a.Close()))
		sleepWhile(&mu, func() bool { return len(s.conns) != 0 })
	}
}

func TestPacketReadTimeout(t *testing.T) {
	t.Parallel()
	a, b := connPair()
	_, err := a.Read(nil)
	qt.Assert(t, qt.StringContains(err.Error(), "timeout"))
	t.Log(err)
	t.Log(a.Close())
	t.Log(b.Close())
}

func sleepWhile(l sync.Locker, cond func() bool) {
	sleepWhileTimeout(l, cond, -1)
	for {
		l.Lock()
		val := cond()
		l.Unlock()
		if !val {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func sleepWhileTimeout(l sync.Locker, cond func() bool, timeout time.Duration) {
	var deadline time.Time
	if timeout >= 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		l.Lock()
		val := cond()
		l.Unlock()
		if !val {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMain(m *testing.M) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		WriteStatus(w)
	})
	setDefaultTestingDurations()
	code := m.Run()
	sleepWhileTimeout(&mu, func() bool {
		return len(sockets) != 0
	}, time.Second)
	mu.Lock()
	numSockets := len(sockets)
	mu.Unlock()
	if numSockets != 0 {
		code = 1
		WriteStatus(os.Stderr)
	}
	os.Exit(code)
}

func TestAcceptReturnsAfterClose(t *testing.T) {
	s, err := NewSocket("", "")
	qt.Assert(t, qt.IsNil(err))
	go s.Close()
	_, err = s.Accept()
	t.Log(err)
}

func TestWriteClose(t *testing.T) {
	a, b := connPair()
	defer a.Close()
	defer b.Close()
	a.Write([]byte("hiho"))
	a.Close()
	c, err := ioutil.ReadAll(b)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(c), "hiho"))
	b.Close()
}

// Check that Conn.Write fails when the PacketConn that Socket wraps is
// closed.
func TestWriteUnderlyingPacketConnClosed(t *testing.T) {
	pc, err := listenPacket("inproc", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	defer pc.Close()
	s, err := NewSocketFromPacketConn(pc)
	qt.Assert(t, qt.IsNil(err))
	defer s.Close()
	dc, ac := connPairSocket(s)
	defer dc.Close()
	defer ac.Close()
	pc.Close()
	n, err := ac.Write([]byte("hello"))
	qt.Check(t, qt.Equals(n, 0))
	// It has to fail. I think it's a race between us writing to the real
	// PacketConn and getting "closed", and the Socket destroying itself, and
	// we get it's destroy error.
	qt.Check(t, qt.IsNotNil(err))
	_, err = dc.Read(nil)
	qt.Check(t, qt.ErrorMatches(err, regexp.QuoteMeta("Socket destroyed")))
}

func TestFillBuffers(t *testing.T) {
	a, b := connPair()
	defer b.Close()
	var sent []byte
	for {
		buf := make([]byte, 100000)
		io.ReadFull(rand.Reader, buf)
		a.SetWriteDeadline(time.Now().Add(5 * time.Second))
		n, err := a.Write(buf)
		sent = append(sent, buf[:n]...)
		if err != nil {
			// Receiver will stop processing packets, packets will be dropped,
			// and thus not acked.
			qt.Check(t, qt.Equals[error](err, errAckTimeout))
			break
		}
		qt.Assert(t, qt.Not(qt.Equals(n, 0)))
	}
	t.Logf("buffered %d bytes", len(sent))
	a.Close()
	all, err := ioutil.ReadAll(b)
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(len(all), len(sent)))
	qt.Check(t, qt.DeepEquals(all, sent))
}

func TestConnLocalRemoteAddr(t *testing.T) {
	a, b := connPair()
	qt.Check(t, qt.Equals(a.LocalAddr().Network(), "utp/inproc"))
	qt.Check(t, qt.Equals(a.RemoteAddr().Network(), "utp/inproc"))
	qt.Check(t, qt.Equals(b.LocalAddr().Network(), "utp/inproc"))
	qt.Check(t, qt.Equals(b.RemoteAddr().Network(), "utp/inproc"))
	qt.Check(t, qt.Equals(b.RemoteAddr().String(), a.LocalAddr().String()))
	qt.Check(t, qt.Equals(a.RemoteAddr().String(), b.LocalAddr().String()))
	a.Close()
	b.Close()
	udpConn, err := net.ListenPacket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	udpSock, err := NewSocketFromPacketConn(udpConn)
	qt.Assert(t, qt.IsNil(err))
	a, b = connPairSocket(udpSock)
	udpSock.Close()
	qt.Check(t, qt.Equals(a.LocalAddr().Network(), "utp/udp"))
	qt.Check(t, qt.Equals(a.RemoteAddr().Network(), "utp/udp"))
	qt.Check(t, qt.Equals(b.LocalAddr().Network(), "utp/udp"))
	qt.Check(t, qt.Equals(b.RemoteAddr().Network(), "utp/udp"))
	qt.Check(t, qt.Equals(b.RemoteAddr().String(), a.LocalAddr().String()))
	qt.Check(t, qt.Equals(a.RemoteAddr().String(), b.LocalAddr().String()))
	a.Close()
	b.Close()
}

func BenchmarkEchoLongBuffer(tb *testing.B) {
	pristine := make([]byte, 3000000)
	n, err := io.ReadFull(rand.Reader, pristine)
	qt.Assert(tb, qt.Equals(n, len(pristine)))
	qt.Assert(tb, qt.IsNil(err))
	tb.SetBytes(int64(len(pristine)))
	tb.ResetTimer()
	for range iter.N(tb.N) {
		func() {
			a, b := connPair()
			defer a.Close()
			defer b.Close()
			go func() {
				n, err := io.Copy(b, b)
				qt.Assert(tb, qt.IsNil(err))
				qt.Assert(tb, qt.Equals(n, int64(len(pristine))))
				b.Close()
			}()
			go func() {
				n, err := a.Write(pristine)
				qt.Assert(tb, qt.IsNil(err))
				qt.Assert(tb, qt.Equals(n, len(pristine)))
			}()
			echo := make([]byte, len(pristine))
			n, err := io.ReadFull(a, echo)
			a.Close()
			qt.Check(tb, qt.IsNil(err))
			qt.Assert(tb, qt.Equals(n, len(echo)))
			qt.Assert(tb, qt.IsTrue(bytes.Equal(pristine, echo)))
		}()
	}
}

// Create a utp.Conn between two Sockets. Then axe one before it can cry for
// help. Check that the other still destroys its underlying UDP PacketConn
// when the Socket and Conn are closed.
func TestSocketDestroyedConnsClosedTimeout(t *testing.T) {
	s1pc, err := net.ListenPacket("udp", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	s1, err := NewSocketFromPacketConnNoClose(s1pc)
	qt.Assert(t, qt.IsNil(err))
	s2, err := NewSocket("", "localhost:0")
	qt.Assert(t, qt.IsNil(err))
	accepted := make(chan struct{})
	var s1c net.Conn
	go func() {
		var err error
		s1c, err = s1.Accept()
		close(accepted)
		qt.Assert(t, qt.IsNil(err))
	}()
	s2c, err := s2.Dial(s1.Addr().String())
	qt.Assert(t, qt.IsNil(err))
	<-accepted
	// Axe Socket 1's PacketConn.
	s1pc.Close()
	// Now its wrappers, that may be confused right now.
	s1.Close()
	// s1 should have been destroyed.
	<-s1.destroyed.LockedChan(&mu)
	s1c.Close()
	// Check that we can now listen in Socket 1's place.
	for {
		pc, err := net.ListenPacket("udp", s1pc.LocalAddr().String())
		if err == nil {
			pc.Close()
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Maybe bump up the unacked sends a bit.
	s2c.Write([]byte("ping"))
	s2c.Write([]byte("pong"))
	s2c.Close()
	s2.Close()
	// s2 should be destroyed when the write timeout on s2c occurs.
	<-s2.destroyed.LockedChan(&mu)
}

// Make sure that creating two connecting sockets repeatedly will successfully
// close the sockets after calling CloseNow(). New sockets can be created with
// the same address without a "bind: address already in use" error.
func TestCloseNow(t *testing.T) {
	s1Addr := "localhost:0"
	s2Addr := "localhost:0"
	for i := 0; i < 100; i++ {
		s1, err := NewSocket("udp", s1Addr)
		qt.Check(t, qt.IsNil(err))
		s2, err := NewSocket("udp", s2Addr)
		qt.Check(t, qt.IsNil(err))
		s1Addr = s1.Addr().String()
		s2Addr = s2.Addr().String()
		go func() {
			c, err := s1.Dial(s2.Addr().String())
			qt.Check(t, qt.IsNil(err))
			_, err = c.Write([]byte("ping"))
			qt.Check(t, qt.IsNil(err))
			_, err = c.Read(nil)
			qt.Check(t, qt.Equals(err.Error(), "EOF"))
		}()
		c, _ := s2.Accept()
		buf := make([]byte, 4)
		_, err = c.Read(buf)
		qt.Check(t, qt.IsNil(err))
		s1.CloseNow()
		s2.CloseNow()
		_, err = c.Read(nil)
		qt.Check(t, qt.Equals(err.Error(), "EOF"))
	}
}

// Test dial 0.0.0.0
func TestZerosIPV4(t *testing.T) {
	//if runtime.GOOS == "windows" {
	//	t.Skip("dialling 0.0.0.0 not working on windows before go1.8?")
	//}

	testSimpleRead(t, "0.0.0.0", "0.0.0.0")
}

// Test dial [::]
func TestZerosIPV6(t *testing.T) {
	//if runtime.GOOS == "windows" {
	//	t.Skip("dialling 0.0.0.0 not working on windows before go1.8?")
	//}

	testSimpleRead(t, "[::]", "[::]")
}

// tests a simple server accept/write/close with client dial/read.
func testSimpleRead(t *testing.T, serverBindIP string, clientDialIP string) {
	l, err := NewSocket("udp", fmt.Sprintf("%s:0", serverBindIP))
	if err != nil {
		t.Fatal(err)
	}
	sport := l.pc.(*net.UDPConn).LocalAddr().(*net.UDPAddr).Port

	defer func() {
		err = l.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	serr := make(chan error, 1)
	go func() {
		con, err := l.Accept()
		if err != nil {
			serr <- err
			return
		}
		io.WriteString(con, "hello")
		serr <- con.Close()
	}()

	client, err := Dial(fmt.Sprintf("%s:%d", clientDialIP, sport))
	if err != nil {
		t.Fatal(err)
	}

	response, err := ioutil.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}

	if string(response) != "hello" {
		t.Fatalf("unexpected response %s", response)
	}

	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNettestInprocSocket(t *testing.T) {
	nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
		s, err := NewSocket("inproc", ":0")
		if err != nil {
			return
		}
		c1, c2 = connPairSocket(s)
		stop = func() {
			s.CloseNow()
		}
		return
	})
}

func TestNettestLocalhostUDP(t *testing.T) {
	t.Skip("flaky")
	nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
		s, err := NewSocket("udp", "localhost:0")
		if err != nil {
			return
		}
		c1, c2 = connPairSocket(s)
		stop = func() {
			s.CloseNow()
		}
		return
	})
}
