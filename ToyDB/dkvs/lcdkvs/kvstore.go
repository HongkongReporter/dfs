package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Constants
const BaseKVSFileName = "kvsfile.data"

//const entryHeaderSize = binary.MaxVarintLen64

type kvs struct {
	index map[string][]byte // Indexes in RAM, (Key, Value) pair
	// This kvs struct uses hashtable, so it is quite quick but
	// a. When the requests strings are within the physical memories: OK, RW take about
	// several hundred ns, but could be troublesome when the data overflows the RAM and
	// Virtual memories start to write the data to disks
	// b. This can only handle the strings within certain size, but big files
	// will mess up with the RAM padding. In report I outline a better approach
	dir    string
	file   *os.File
	offset int64
	mu     sync.Mutex
	//massager chan msg
	//mutlock sync.RWMutex
}

type dkvs struct {
	mu        sync.Mutex
	port      string
	ds        *kvs
	nServer   int
	done      bool
	billboard map[int]msg
}

// bool has default false
// active: if some process is broadcasting
// me: the index of the sender
// ok: received
type msg struct {
	nRemain int // n Remaning write operations
	key     string
	value   []byte
}

func (store *kvs) Read(m map[string][]byte, offset int64) (err error) {
	//store.mu.Lock()
	//defer store.mu.Unlock()
	iobuf, err := os.ReadFile(store.dir)
	//testing
	if err != nil {
		return err
	}
	//fmt.Println(iobuf)
	if len(iobuf) == 0 {
		return
	}
	if err := json.Unmarshal(iobuf, &store.index); err != nil {
		panic(err)
	}
	return
}

func (store *kvs) WriteToDisk() (err error) {
	//store.mu.Lock()
	//defer store.mu.Unlock()
	binaryMap, err := json.Marshal(store.index)
	if err != nil {
		log.Fatal(err)
		fmt.Println("Error when marshalling")
		return err
	}

	//empty the file...
	if err := os.Truncate(store.dir, 0); err != nil {
		return err
	}
	err = os.WriteFile(store.dir, binaryMap, 0666)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return
}

func Open(dir string) (*kvs, error) {
	//if the directory does not exist: create one
	//open the file... load into file and offset
	file, err := os.OpenFile(dir, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	newKVS := &kvs{
		//offset: stat.Size(),
		offset: 0,
		index:  make(map[string][]byte),
		dir:    dir,
		file:   file,
	}
	//write the previous saved content from last session...
	err = newKVS.Read(newKVS.index, newKVS.offset)
	if err != nil {
		fmt.Println(err)
	}
	newKVS.WriteToDisk()
	return newKVS, nil
}

func (store *kvs) Get(key string) (val []byte, err error) {
	//store.mu.Lock()
	//defer store.mu.Unlock()
	val = store.index[key]
	return val, nil
}

func (store *kvs) Set(key string, value []byte) (err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(key) == 0 || len(value) == 0 {
		return err
	}
	store.index[key] = value
	err = store.WriteToDisk()
	if err != nil {
		return err
	}
	log.Println(">> The server has written: " + key + " " + string(value))
	return nil
}

func Reply(message string, connection net.Conn) (err error) {
	n, err := connection.Write([]byte(message))
	if len([]byte(message)) < n || err != nil {
		return err
	}
	return nil
}

// cook up a file name for i-th replicas
func CookUpDir(dir string, i int) string {
	s := dir + "/data-"
	s += strconv.Itoa(i)
	s += ".log"
	return s
}

// cook up socket port for n replicas
func CookUpSock(i int) string {
	s := "/var/tmp/dkvs-"
	return s + strconv.Itoa(i)
}

func OpenAndListen(dir string, PORT string, n int) {
	server := &dkvs{}
	server.nServer = n
	server.port = "/var/tmp/dkvs-" + PORT
	server.done = false
	PORT_NUMBER, _ := strconv.Atoi(PORT)
	dir = CookUpDir(dir, PORT_NUMBER)

}

func (server *dkvs) Done() bool {
	if server.done {
		return true
	} else {
		return false
	}
}
func (server *dkvs) Write(Args *msg, Reply *msg) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.ds.index[Args.key] = Args.value
	err := server.ds.WriteToDisk()
	if err != nil {
		return err
	} else {
		Reply.nRemain -= 1
		return nil
	}
}

//Broadcaster for i-th server
// return false if it fails
func (server *dkvs) Broadcaster(key string, value []byte) bool {
	Args := msg{server.nServer, key, value}
	Reply := msg{server.nServer, key, value}
	for k := 0; k < server.nServer; k++ {
		// calling the respective sockets
		sock := CookUpSock(k)
		succ := call("dkvs.Write", Args, Reply, sock)
		if !succ {
			return false
		}
	}
	return true
}

// i the i-th replica
func (server *dkvs) runReplicas(i int, ear net.Listener, wg *sync.WaitGroup) {
	//server.serverList[i].mu.Lock()
	//defer server.serverList[i].mu.Unlock()

	defer wg.Done()
	signal := make(chan bool)

	// first, initialize the messager channel
	go server.Broadcaster(i)

	for {
		connection, err := ear.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go server.handleRequests(i, signal, connection)
		if isKilled := <-signal; isKilled {
			log.Println(">> The server will quit...")
			return
		}
		time.Sleep(25 * time.Millisecond) // to prevent deadlock...
	}
	close(signal)
}

// i: sender
// The sender billboard the set message
/*
func (server *dkvs) Billboard(i int, done chan bool, key string, value []byte, write bool) {
	server.serverList[i].mu.Lock()
	if write {
		for k := 0; k < server.nServer; k++ {
			if k != i {
				server.billboard[k] = msg{true, i, key, value, false, true}
			}
		}
	} else {
		for k := 0; k < server.nServer; k++ {
			if k != i {
				server.billboard[k] = msg{true, i, key, value, false, false}
			}
		}
	}
	server.serverList[i].mu.Unlock()
	time.Sleep(time.Second)
	isDone := true
	for k := 0; k < server.nServer; k++ {
		if k != i {
			m := server.billboard[k]
			isDone = m.ok && isDone
		}
	}
	done <- isDone
}
*/

func (server *dkvs) handleRequests(i int, signal chan bool, connection net.Conn) {
	//server.serverList[i].mu.Lock()
	//defer server.serverList[i].mu.Unlock()
	defer connection.Close()
	for {
		CommandReader := bufio.NewReader(connection)
		data, err := CommandReader.ReadString('\n')
		switch err {
		case io.EOF:
			fmt.Print(">> The Client has lost the connection")
			return
		case nil:
			log.Printf(">> Receiving client commands...")
		default:
			log.Printf(">> go routine error: %v", err)
			return
		}
		log.Println(">> CONNECTION ESTABLISHED")
		info := strings.Split(string(data), " ")
		command := info[0]
		log.Printf(">> The client requests " + command)
		switch {
		case command == "get":
			key := info[1]

			// Broadcast....
			done := make(chan bool)
			go server.Billboard(i, done, key, []byte(""), false)
			//go chan will wait until some data is sent in the pipe
			// So this will be executed only if all acks are collected...
			if isDone := <-done; isDone {
				msg := string(server.serverList[i].index[key]) + "\n"
				fmt.Print(">> Client requests value " + msg)
				Reply(msg, connection)
			} else {
				Reply("Read fails\r\n", connection)
			}
			signal <- false
			return
		case command == "set":
			key := info[1]
			//Read nbytes worth of value
			value := []byte(info[2])
			// Broadcast....
			done := make(chan bool)
			go server.Billboard(i, done, key, value, true)
			//go chan will wait until some data is sent in the pipe
			// So this will be executed only if all acks are collected...
			if isDone := <-done; isDone {
				err = server.serverList[i].Set(key, value)
				if err != nil {
					Reply("Error: Cannot save data\r\n", connection)
				} else {
					Reply("Written SUCCESS\r\n", connection)
				}
			} else {
				Reply("Write fails\r\n", connection)
			}
			signal <- false
			return
		case command == "kill":
			signal <- true
			return
		default:
			Reply("INVALID COMMAND: "+string(data)+"\r\n", connection)
			signal <- false
			return
		}
	}
}

func call(rpcname string, args interface{}, reply interface{}, PortName string) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+PortName)
	//sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", PortName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (store *dkvs) ListenRPC() {
	rpc.Register(store)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/dkvs-"
	s += strconv.Itoa(os.Getuid())
	return s
}
