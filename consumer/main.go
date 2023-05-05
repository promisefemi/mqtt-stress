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
	num int
}

var clientIDs = make([]string, 0)

func main() {
	broker := flag.String("b", "127.0.0.1:8000", "Broker IP Address")
	numOfSensors := flag.Int("s", 1, "Number of clients to simulate")
	qos := flag.Int("q", 1, "QOS level")
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
			options := paho.ClientConfig{
				Conn: conn,
				Router: paho.NewSingleHandlerRouter(func(publish *paho.Publish) {
					messageCount.Mutex.Lock()
					messageCount.num++
					messageCount.Mutex.Unlock()
					fmt.Printf("message from broker topic: %s -- message %s \n", publish.Topic, publish.Payload)
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
	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	<-ic

	fmt.Printf("Number of messages received:  %d\n", messageCount.num)

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
