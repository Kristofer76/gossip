package main

import (
   "bufio"
   "fmt"
   "net"
   "encoding/json"
   "os"
   "time"
   "math/rand"
   "strconv"
   "sync"
)

var globalMessageId int = 20
var idLock sync.Mutex

type Message struct {
	Id int
	MType string
	Sender int
	Origin int
	Data string
}

/* A Simple function to verify error */
func CheckError(err error) {
   if err  != nil {
	   fmt.Println("Error: " , err)
	   os.Exit(0)
   }
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
	  return nil, err
	}
	defer file.Close()
  
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
	  lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
  }

func sendAround(conn *net.UDPConn, msg Message, ports []int, myport int, ttl int, t time.Duration) {	
	smsg, err := json.Marshal(msg)
	CheckError(err)
	
	buf := []byte(smsg)	
	
	for i := 0; i < ttl ; i++ {
		pNum := rand.Intn(len(ports))
		ReceiverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+strconv.Itoa(ports[pNum]))


		CheckError(err)
			
		_,err = conn.WriteToUDP(buf, ReceiverAddr)
		CheckError(err)
		
		time.Sleep(time.Millisecond * t)
	}
}


func Index(vs []int, t int) int {
    for i, v := range vs {
        if v == t {
            return i
        }
    }
    return -1
}


func Include(vs []int, t int) bool {
    return Index(vs, t) >= 0
}

func Filter(vs []int, f func(int) bool) []int {
    vsf := make([]int, 0)
    for _, v := range vs {
        if f(v) {
            vsf = append(vsf, v)
        }
    }
    return vsf
}

func deletePorts(ports []int, portsToDelete []int) ([]int){
	var filteredPorts []int = Filter(ports, func(v int) bool {
        return !(Include(portsToDelete, v))
	})
	return filteredPorts
}

func processBody (self Node, ttl int,tstr int, neighbours []Node, nodesCount int, wg *sync.WaitGroup){
	fmt.Println("Routine ran")
	
	pid := self.id
	myport := strconv.Itoa(self.port)

	t := time.Duration(tstr)	
	
	var ports []int
	for _, element := range neighbours {
		ports = append(ports, element.port)
	}

	buf := make([]byte, 1024)

	myPortInt,err := strconv.Atoi(myport)
	CheckError(err)
	
	fPorts := deletePorts(ports,[]int{myPortInt})
		
	MyAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+myport)
	CheckError(err)
	ServerConn, err := net.ListenUDP("udp", MyAddr)
	CheckError(err)

	var receivedMsgs []int
	expectedConfCount := nodesCount - 1
	startTime := time.Now()
	if myport == "10001" {
		
		fmt.Println("gossip begins")			
		
		myMsg := Message {
			globalMessageId,
			"multicast",
			0,
			0,
			"gossip",
		}
		idLock.Lock()
		globalMessageId++
		idLock.Unlock()
		receivedMsgs = append(receivedMsgs, myMsg.Id)		
		sendAround(ServerConn, myMsg, fPorts, myPortInt, ttl, t)
	}
	
	for {
		n,addr,err := ServerConn.ReadFromUDP(buf)
		CheckError(err)
		
		msg := Message{}
		err = json.Unmarshal(buf[0:n], &msg)
		CheckError(err)
		
		//fmt.Println(myport, "Received ", msg.Id, " from ",  addr)			
		
		if (!Include(receivedMsgs, msg.Id)) {
			receivedMsgs = append(receivedMsgs, msg.Id)
			neighbours := deletePorts(fPorts,[]int{addr.Port})

			msg.Sender = pid 
			go sendAround(ServerConn, msg, neighbours, myPortInt, ttl, t)		
			
			if msg.MType == "multicast" {
				nMsg := Message{}
				nMsg.Id = globalMessageId
				idLock.Lock()
				globalMessageId++
				idLock.Unlock()
				nMsg.Origin = pid
				nMsg.Sender = pid
				nMsg.MType = "notification"

				go sendAround(ServerConn, nMsg, fPorts, myPortInt, ttl, t)						
			} else {
				expectedConfCount--
				//fmt.Println(expectedConfCount, "least")
				if myport == "10001" {
					fmt.Println(expectedConfCount)					
				}	
				if expectedConfCount == 0 {
					break							
				}
			}
		}
	}
	if myport == "10001" {
		fmt.Println((time.Now().Sub(startTime))/t)		
		
	}		
	//fmt.Println("I've done")		
	wg.Done()
}

func main() {

	ttl, err := strconv.Atoi(os.Args[1])
	CheckError(err)	
	tstr, err := strconv.Atoi(os.Args[2])
	CheckError(err)

	graph := Generate(128, 2, 3, 10001)

	var wg sync.WaitGroup
	wg.Add(len(graph))
	for self, neighbours := range graph {	
		go processBody(self,ttl,tstr,neighbours,128,&wg)
	}
	wg.Wait()
	
}