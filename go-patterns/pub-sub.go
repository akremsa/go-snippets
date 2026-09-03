package gopatterns

import (
	"log"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

type Message struct {
	Topic string
	Data  interface{}
}

type Broker struct {
	mu          sync.RWMutex
	wg          sync.WaitGroup
	subscribers map[string][]*Subscriber
	topicCh     chan Message
	quitCh      chan struct{}
}

type Subscriber struct {
	id     int
	topic  string
	dataCh chan Message
	quitCh chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		subscribers: make(map[string][]*Subscriber),
		topicCh:     make(chan Message),
		quitCh:      make(chan struct{}),
	}

	b.wg.Go(b.eventLoop)
	return b
}

func (b *Broker) Subscribe(topic string, bufferSize int) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	newSubscriber := &Subscriber{
		id:     rand.IntN(10000),
		topic:  topic,
		dataCh: make(chan Message, bufferSize),
		quitCh: make(chan struct{}),
	}
	b.subscribers[topic] = append(b.subscribers[topic], newSubscriber)
	log.Printf("subscriber %d subscribed to topic %s", newSubscriber.id, topic)
	return newSubscriber
}

func (b *Broker) Unsubscribe(s *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	log.Printf("Unsubsribing %d from topic %s", s.id, s.topic)
	subs := b.subscribers[s.topic]
	for i, subToRemove := range subs {
		if subToRemove.id == s.id {
			b.subscribers[s.topic] = slices.Delete(subs, i, i+1)
			break
		}
	}
	close(s.quitCh)
}

func (b *Broker) eventLoop() {
	log.Print("starting event loop")
	for {
		select {
		case msg := <-b.topicCh:
			b.broadcast(msg)

		case <-b.quitCh:
			log.Print("exiting event loop...")
			return
		}
	}
}

func (b *Broker) broadcast(msg Message) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subs, ok := b.subscribers[msg.Topic]
	if !ok {
		log.Printf("No subscribers found for a topic: %s", msg.Topic)
		return
	}

	for _, s := range subs {
		select {
		case s.dataCh <- msg:
			log.Printf("Broker: sent message to subscriber %d\n", s.id)
		default:
			log.Printf("Broker: subscriber %d channel full, dropping message\n", s.id)
		}
	}
}

func (b *Broker) Publish(topic string, data interface{}) {
	msg := Message{Topic: topic, Data: data}

	select {
	case b.topicCh <- msg:
		log.Printf("Broker: published message to topic %s\n", topic)
	case <-b.quitCh:
		log.Print("Broker: can not accept new message, shutting down")
	}
}

func (b *Broker) Shutdown() {
	log.Print("Broker shutdown...")
	close(b.quitCh)
	defer b.wg.Wait()

	for topic, subs := range b.subscribers {
		for _, sub := range subs {
			close(sub.quitCh)
			close(sub.dataCh)
		}
		log.Printf("Broker: Cleaned up topic '%s'\n", topic)
	}
	log.Println("Broker: Shutdown complete")
}

func (s *Subscriber) Start() {
	for {
		select {
		case data, ok := <-s.dataCh:
			if !ok {
				log.Printf("subscriber %d: data channel is closed, exiting...", s.id)
				return
			}
			log.Printf("subscriber %d received message: %v", s.id, data)

		case <-s.quitCh:
			log.Printf("subscriber %d: closing...", s.id)
			return

		}
	}
}

func RunPubSub() {
	const newsTopic = "news"
	const moviesTopic = "movies"
	const sportTopic = "sport"

	broker := NewBroker()
	subNews1 := broker.Subscribe("news", 15)
	subMovies1 := broker.Subscribe("movies", 20)
	subNews2 := broker.Subscribe("news", 15)
	subSport1 := broker.Subscribe("sport", 10)

	go subNews1.Start()
	go subMovies1.Start()
	go subNews2.Start()
	go subSport1.Start()

	time.Sleep(100 * time.Millisecond)

	broker.Publish(newsTopic, "Breaking news")
	broker.Publish(moviesTopic, "Matrix movie")
	broker.Publish(sportTopic, "Footbal world cup")
	broker.Publish(newsTopic, "Weather forecast")
	broker.Unsubscribe(subNews2)
	time.Sleep(100 * time.Millisecond)
	broker.Publish(newsTopic, "Local daily news")
	time.Sleep(100 * time.Millisecond)

	broker.Shutdown()
}
