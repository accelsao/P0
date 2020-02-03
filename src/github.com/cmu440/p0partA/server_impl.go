package p0partA

// Implementation of a KeyValueServer. Students should write their code in this file.
// Implementation of a KeyValueServer.

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/cmu440/p0partA/kvstore"
	"net"
	"strconv"
)

const MAX_MESSAGE_QUEUE_LENGTH = 500

const (
	T_PUT    = 0
	T_GET    = 1
	T_DELETE = 2
)

type Client struct {
	connection net.Conn
	quitSignal_Write chan int
	messageQueue chan []byte
}

// Used to specify DBRequests
type db struct {
	qtype int
	key   string
	value []byte
}

type db_conn struct {
	db_v       db
	clin Client
}

// Implements KeyValueServer.
type keyValueServer struct {
	listener       net.Listener
	currentClients []Client




	newConnection chan net.Conn

	deadClient chan Client
	dbQuery_1  chan *db_conn
	dbResponse map[net.Conn]chan []byte

	// writeChan chan []byte

	countDroppedClient chan int
	countDropped int
	countDroppedNum chan int


	countClients    chan int //这两个有什么区别
	clientCount     chan int

	quitSignal_Main   chan int //通过通道的方式发送信号给routine， 通知其结束
	quitSignal_Accept chan int //通过通道的方式发送信号给routine， 通知其结束

	store kvstore.KVStore // kvstore的具体实现
}

// Initializes a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {

	var kvs keyValueServer

	kvs.newConnection = make(chan net.Conn)

	kvs.deadClient = make(chan Client)

	kvs.dbQuery_1 = make(chan *db_conn)
	kvs.dbResponse = make(map[net.Conn]chan []byte)


	kvs.countDroppedClient = make(chan int)
	kvs.countDropped = 0
	kvs.countDroppedNum = make(chan int)

	kvs.countClients = make(chan int)
	kvs.clientCount = make(chan int)

	kvs.quitSignal_Main = make(chan int)

	kvs.quitSignal_Accept = make(chan int)
	kvs.store = store

	return &kvs

}

// Implementation of Start for keyValueServer.
func (kvs *keyValueServer) Start(port int) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}

	kvs.listener = ln

	go runServer(kvs)
	go acceptRoutine(kvs)

	return nil
}

// Implementation of Close for keyValueServer.
func (kvs *keyValueServer) Close() {
	kvs.listener.Close()
	kvs.quitSignal_Main <- 0
	kvs.quitSignal_Accept <- 0
}

func (kvs *keyValueServer) CountActive() int {
	kvs.countClients <- 1 // signal countClients
	return <- kvs.clientCount
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.countDroppedClient <- 1 // signal countDroppedClient
	return <- kvs.countDroppedNum
}

// Main server routine.
func runServer(kvs *keyValueServer) {
	for {
		select {
		// Get the number of clients.
		case <-kvs.countClients:
			kvs.clientCount <- len(kvs.currentClients)

		// Get the number of dropped clients.
		case  <- kvs.countDroppedClient:
			kvs.countDroppedNum <- kvs.countDropped

		// Add a new client
		case conn := <-kvs.newConnection:
			c := Client{
				connection: conn,
				quitSignal_Write: make(chan int),
				messageQueue: 	make(chan []byte, 500),
			}
			kvs.currentClients = append(kvs.currentClients, c)
			go readRoutine(kvs, c)
			go writeRoutine(kvs, c)


		// Remove the dead client.
		case deadClient := <-kvs.deadClient:
			for i, c := range kvs.currentClients {
				if c == deadClient {
					kvs.currentClients = append(kvs.currentClients[:i], kvs.currentClients[i+1:]...)
					kvs.countDropped++
					break
				}
			}

		case request := <-kvs.dbQuery_1:
			if request.db_v.qtype == T_GET {
				//fmt.Printf("dbQuerry type: Get:%s\n", request.db_v.key)
				value := kvs.store.Get(request.db_v.key)
				for _, v := range value {
					go func(v []byte) {
						select {
						case request.clin.messageQueue <- []byte(fmt.Sprintf("%s:%s", request.db_v.key, v)):
						default:
						}
					}(v)
					//request.clin.messageQueue <- []byte(fmt.Sprintf("%s:%s", request.db_v.key, v))
				}
			} else if request.db_v.qtype == T_PUT {
				//fmt.Printf("dbQuerry type: Put:%s:%s\n", request.db_v.key, request.db_v.value)
				kvs.store.Put(request.db_v.key, request.db_v.value)
			} else if request.db_v.qtype == T_DELETE {
				//fmt.Printf("dbQuerry type: Delete:%s\n", request.db_v.key)
				kvs.store.Clear(request.db_v.key)
			}




		// End each client routine.
		case <-kvs.quitSignal_Main:
			for _, c := range kvs.currentClients {
				c.connection.Close()
				c.quitSignal_Write <- 0
				c.quitSignal_Read <- 0
			}
			return
		}
	}
}

// One running instance; accepts new clients and sends them to the server.
func acceptRoutine(kvs *keyValueServer) {
	for {
		select {
		case <-kvs.quitSignal_Accept:
			return
		default:
			conn, err := kvs.listener.Accept()
			//为每个新建的 client 创建两个用于 数据查询 的通道
			if err == nil {
				kvs.newConnection <- conn
			}
		}
	}
}

// One running instance for each client; reads in
// new  messages and sends them to the server. 读取指令

func readRoutine(kvs *keyValueServer, c Client) {
	buf := bufio.NewReader(c.connection)
	for {
		message, err := buf.ReadBytes('\n')
		if err != nil {
			kvs.deadClient <- c
			c.quitSignal_Write <- 1
			return
		}
		tokens := bytes.Split(message, []byte(":"))
		if string(tokens[0]) == "Put" {

			key := string(bytes.TrimSuffix(tokens[1], []byte("\n")))

			db_v := db{
				qtype: T_PUT,
				key:   key,
				value: tokens[2],
			}

			kvs.dbQuery_1 <- &db_conn{
				db_v:       db_v,
				clin: 		c,
			}

		} else if string(tokens[0]) == "Get" {

			key := string(bytes.TrimSuffix(tokens[1], []byte("\n")))

			db_v := db{
				qtype: T_GET,
				key:   key,
			}

			kvs.dbQuery_1 <- &db_conn{
				db_v:       db_v,
				clin: 		c,
			}

		} else if string(tokens[0]) == "Delete" {
			key := string(bytes.TrimSuffix(tokens[1], []byte("\n")))
			db_v := db{
				qtype: T_DELETE,
				key:   key,
			}

			kvs.dbQuery_1 <- &db_conn{
				db_v:       db_v,
				clin: 		c,
			}

		}
	}
}

func writeRoutine(kvs *keyValueServer, c Client) {
	for {
		select {
		case <-c.quitSignal_Write:
			return
		case item := <- c.messageQueue:
			if _, err := c.connection.Write(item); err != nil {
				fmt.Println("error happens, server_impl.go:317")
			}
		default:
		}
	}
}
