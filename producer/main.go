package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/eclipse/paho.golang/paho"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type counter struct {
	sync.Mutex
	num, total int
}

var clientIDs = make([]string, 0)

func main() {
	broker := flag.String("b", "127.0.0.1:8000", "Broker IP Address")
	transportLayer := flag.String("l", "tcp", "Connection transport layer")
	timeToRun := flag.Int("t", 10, "Time to run in seconds")
	numOfSensors := flag.Int("s", 1, "Number of clients to simulate")
	qos := flag.Int("q", 1, "QOS level")
	messageInterval := flag.Int("p", 5, "Interval for each connection to send message")
	flag.Parse()

	var brokerIP *net.TCPAddr
	var unixDomainSocket string
	var err error
	if *transportLayer == "tcp" {
		brokerIP, err = net.ResolveTCPAddr("tcp", *broker)
		if err != nil {
			log.Fatalln("Invalid broker ip address, ip address should contain port")
		}
	} else if *transportLayer == "uds" {
		unixDomainSocket = *broker
	}

	messageCount := &counter{}

	//processRunTime := time.After()
	//fmt.Println(processRunTime)

	for i := 0; i < *numOfSensors; i++ {
		go func() {
			var conn net.Conn
			var err error

			if *transportLayer == "tcp" {
				tcpServer := fmt.Sprintf("%s:%d", brokerIP.IP.String(), brokerIP.Port)
				conn, err = net.Dial("tcp", tcpServer)
			} else {
				conn, err = net.Dial("unix", unixDomainSocket)
			}

			if err != nil {
				fmt.Printf("unable to connect to broker - %s\n", err)
				return
			}
			options := paho.ClientConfig{
				Conn: conn,
				Router: paho.NewSingleHandlerRouter(func(publish *paho.Publish) {
					fmt.Printf(publish.Topic)
				}),
			}

			clientID := generateClientID()
			client := paho.NewClient(options)
			cp := &paho.Connect{
				KeepAlive:  30,
				ClientID:   clientID,
				CleanStart: true,
			}

			ca, err := client.Connect(context.Background(), cp)
			if err != nil {
				fmt.Printf("unable to connect to broker - %s\n", err)
				return
			}
			if ca.ReasonCode != 0 {
				fmt.Printf("failed to connect to %s, Reason code: %d, Reason text: %s\n", conn.RemoteAddr().String(), ca.ReasonCode, ca.Properties.ReasonString)
				return
			}
			fmt.Printf("Connected to %s\n", conn.RemoteAddr().String())

			rand.Seed(time.Now().UnixNano())

			time.Sleep(time.Duration(rand.Intn(1000000)+1) * time.Microsecond)

			ticker := time.NewTicker(time.Duration(*messageInterval) * time.Second)
			for range ticker.C {
				payload := map[string]int64{
					"timestamp": time.Now().UnixMicro(),
					"value":     time.Now().UnixNano(),
				}
				payloadByte, err := json.Marshal(payload)
				if err != nil {
					fmt.Printf("error converting map to json %s\n", err)
				}
				publish := paho.Publish{
					QoS:     byte(*qos),
					Retain:  false,
					Topic:   fmt.Sprintf("2/1/2/%s", clientID),
					Payload: payloadByte,
				}

				if _, err := client.Publish(context.Background(), &publish); err != nil {
					fmt.Printf("error publishing to broker %s", err)
				}
				//fmt.Printf("message sent -  %s from client %s\n", payloadByte, clientID)
				messageCount.Mutex.Lock()
				messageCount.num++
				messageCount.total++
				messageCount.Mutex.Unlock()

			}
			return
		}()

	}
	fmt.Println("Processes are running")
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			var currentMessageNumber int
			messageCount.Mutex.Lock()
			currentMessageNumber = messageCount.num
			messageCount.num = 0
			messageCount.Mutex.Unlock()
			fmt.Printf("Message/s: %d\n", currentMessageNumber)
		}
	}()

	time.Sleep(time.Duration(*timeToRun) * time.Second)
	fmt.Println("Processes run time complete")

	fmt.Printf("Number of messages sent:  %d\n", messageCount.total)

}

func generateClientID() string {
	min := 100000
	max := 10000000000
	rand.Seed(time.Now().UnixNano())
	randomInt := rand.Intn(max-min) + min

	clientID := strconv.Itoa(randomInt)

	if len(clientIDs) > 0 {
		for _, i := range clientIDs {
			if i == clientID {
				return generateClientID()
			}
		}
	}
	clientIDs = append(clientIDs, clientID)
	return clientID
}
