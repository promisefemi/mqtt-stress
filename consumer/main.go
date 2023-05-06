package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/eclipse/paho.golang/paho"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type counter struct {
	sync.Mutex
	num   int
	total int
}

var clientIDs = make([]string, 0)

func main() {
	broker := flag.String("b", "127.0.0.1:8000", "Broker IP Address")
	numOfSensors := flag.Int("s", 1, "Number of clients to simulate")
	qos := flag.Int("q", 1, "QOS level")
	print := flag.Bool("p", false, "Print new messages")
	flag.Parse()

	brokerIP, err := net.ResolveTCPAddr("tcp", *broker)
	if err != nil {
		log.Fatalln("Invalid broker ip address, ip address should contain port")
	}
	topic := "$share/my-shared-subscriber-group/2/1/2/#"
	messageCount := &counter{}
	for i := 0; i < *numOfSensors; i++ {
		go func() {
			tcpServer := fmt.Sprintf("%s:%d", brokerIP.IP.String(), brokerIP.Port)
			//fmt.Println(tcpServer)
			conn, err := net.Dial("tcp", tcpServer)
			if err != nil {
				fmt.Printf("unable to make tcp connection - %s\n", err)
				return
			}
			clientID := generateClientID()

			options := paho.ClientConfig{
				Conn: conn,
				Router: paho.NewSingleHandlerRouter(func(publish *paho.Publish) {
					messageCount.Mutex.Lock()
					messageCount.num++
					messageCount.total++
					messageCount.Mutex.Unlock()
					if *print {
						fmt.Printf("ClientID %s -- message from broker topic: %s -- message %s \n", clientID, publish.Topic, publish.Payload)
					}
				}),
			}

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
			//fmt.Printf("\n%+v\n", ca.Properties)
			if ca.ReasonCode != 0 {
				fmt.Printf("failed to connect to %s, Reason code: %d, Reason text: %s\n", tcpServer, ca.ReasonCode, ca.Properties.ReasonString)
				return
			}
			fmt.Printf("Connected to %s\n", tcpServer)
			if _, err := client.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: map[string]paho.SubscribeOptions{
					topic: {QoS: byte(*qos)},
				},
			}); err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("subscribed to %s\n", topic)
			holdout := make(chan bool)
			<-holdout
		}()
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			var currentMessageNumber int
			messageCount.Mutex.Lock()
			currentMessageNumber = messageCount.num
			messageCount.num = 0
			fmt.Printf("Message/s: %d (%d)\n", currentMessageNumber, messageCount.total)
			messageCount.Mutex.Unlock()
		}
	}()

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	<-ic

	fmt.Printf("\nNumber of messages received:  %d\n", messageCount.total)

	os.Exit(0)
}

func generateClientID() string {
	min := 100
	max := 200
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
