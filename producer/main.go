package main

import (
	"encoding/json"
	"flag"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	messageInterval := flag.Int("p", 1, "Interval for each connection to send message")
	messagePerWorker := flag.Int("m", 1, "Number of messages to send per messageInterval -p")
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

			var brokerConnection string

			options := mqtt.NewClientOptions()
			if *transportLayer == "tcp" {
				brokerConnection = fmt.Sprintf("%s:%d", brokerIP.IP.String(), brokerIP.Port)
			} else {
				brokerConnection = fmt.Sprintf("unix://%s", unixDomainSocket)
			}

			clientID := generateClientID()
			options.AddBroker(brokerConnection)
			options.SetClientID(clientID)
			options.OnConnect = connectHandler
			options.OnConnectionLost = connectionLostHandler

			client := mqtt.NewClient(options)
			token := client.Connect()

			if token.Wait() && token.Error() != nil {
				log.Fatalf("error - unable to connect to server %s", token.Error())
			}

			fmt.Printf("Connected to %s\n", brokerConnection)

			//Internal ticker calculation
			internalTickerDuration := (float64(*messageInterval) / float64(*messagePerWorker)) * float64(1000)

			//Random start timer
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(rand.Intn(1000000)+1) * time.Microsecond)

			ticker := time.NewTicker(time.Duration(*messageInterval) * time.Second)
			for range ticker.C {
				//fmt.Println("Message Ticker")
				internalTicker := time.NewTicker(time.Duration(internalTickerDuration) * time.Millisecond)
				for range internalTicker.C {
					//fmt.Println("Internal ticker run")
					payload := map[string]int64{
						"timestamp": time.Now().UnixMicro(),
						"value":     time.Now().UnixNano(),
					}

					topic := fmt.Sprintf("2/1/2/%s", clientID)

					payloadByte, err := json.Marshal(payload)
					if err != nil {
						fmt.Printf("error converting map to json %s\n", err)
					}

					token := client.Publish(topic, byte(*qos), false, payloadByte)
					if token.Wait(); token.Error() != nil {
						fmt.Printf("error publishing to broker %s\n", token.Error())
					}

					fmt.Printf("message sent -  %s from client %s\n", payloadByte, clientID)
					messageCount.Mutex.Lock()
					messageCount.num++
					messageCount.total++
					messageCount.Mutex.Unlock()
				}
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

func connectHandler(client mqtt.Client) {
	//fmt.Println("Client is connected")
	//client.
}

func connectionLostHandler(client mqtt.Client, err error) {
	fmt.Printf("error: client connection was lost - %s", err)
}
