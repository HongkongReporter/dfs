package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
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
	mu         sync.Mutex
	nServer    int
	serverList map[int]*kvs
	billboard  map[int]msg
}

// bool has default false
// active: if some process is broadcasting
// me: the index of the sender
// ok: received
type msg struct {
	active bool
	me     int
	key    string
	value  []byte
	ok     bool
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
	if len(key) == 0 {
		return nil, err
	}
	if val, ok := store.index[key]; ok {
		return val, nil
	}
	return nil, err
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
func CookUpSock(sock string, i int) string {
	sockNum, _ := strconv.Atoi(sock)
	sockNum += i
	return ":" + strconv.Itoa(sockNum)
}

func OpenAndListen(dir string, PORT string, n int) {
	var wg sync.WaitGroup
	server := &dkvs{}
	server.nServer = 0
	server.serverList = make(map[int]*kvs)
	server.billboard = make(map[int]msg)
	for i := 0; i < n; i++ {
		newDir := CookUpDir(dir, i)
		newKVS, err := Open(newDir)
		if err != nil {
			log.Fatal(err)
			fmt.Println("Cannot resolve persistent storage opening.")
			return
		}
		server.serverList[i] = newKVS
		//server.serverList[i].massager = make(chan msg)
		server.billboard[i] = msg{false, -1, "", []byte(""), false}
		sock := CookUpSock(PORT, i)
		ear, err := net.Listen("tcp", sock)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer ear.Close()
		// wait until done
		// isKilled for detecting if the process requests the server(all servers) to shut down
		wg.Add(1)
		go server.runReplicas(i, ear, &wg)
		fmt.Println(">> Replica: " + strconv.Itoa(i) + " is running... at:" + dir + " with port: " + sock)
		server.nServer += 1
	}
	wg.Wait()
}

//Broadcaster for i-th server
func (server *dkvs) Broadcaster(i int) {

	for {
		// First, listen to their own channel....
		if server.billboard[i].active {
			server.mu.Lock()
			server.serverList[i].Set(server.billboard[i].key, server.billboard[i].value)
			//Yes, we did the write
			m := server.billboard[i]
			server.billboard[i] = msg{false, m.me, "", []byte(""), true}
			server.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
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
func (server *dkvs) Billboard(i int, done chan bool, key string, value []byte) {
	server.serverList[i].mu.Lock()
	for k := 0; k < server.nServer; k++ {
		if k != i {
			server.billboard[k] = msg{true, i, key, value, false}
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
			val, err := server.serverList[i].Get(key)
			if err != nil {
				Reply("Cannot Find Key!\r\n", connection)
			}
			msg := string(val) + "\n"
			fmt.Print(">> Client requests value " + msg)
			Reply(msg, connection)
			signal <- false
			return
		case command == "set":
			key := info[1]
			//Read nbytes worth of value
			value := []byte(info[2])
			// Broadcast....
			done := make(chan bool)
			go server.Billboard(i, done, key, value)
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
