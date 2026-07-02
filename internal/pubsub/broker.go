package pubsub

import (
	"log"
	"sync"
)

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type Broker struct {
	mu          sync.RWMutex
	channels    map[string]*Channel
	subscribers map[string]map[string]*Subscriber
}

type Channel struct {
	Name        string
	subscribers map[string]*Subscriber
	mu          sync.RWMutex
}

type Subscriber struct {
	ID     string
	Ch     chan Message
	closed bool
	mu     sync.Mutex
}

type Message struct {
	Channel string
	Data    []byte
}

func NewBroker() *Broker {
	return &Broker{
		channels:    make(map[string]*Channel),
		subscribers: make(map[string]map[string]*Subscriber),
	}
}

func (b *Broker) Subscribe(channelName, subscriberID string) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.channels[channelName]
	if !ok {
		ch = &Channel{
			Name:        channelName,
			subscribers: make(map[string]*Subscriber),
		}
		b.channels[channelName] = ch
	}

	sub := &Subscriber{
		ID: subscriberID,
		Ch: make(chan Message, 100),
	}

	ch.mu.Lock()
	ch.subscribers[subscriberID] = sub
	ch.mu.Unlock()

	if b.subscribers[channelName] == nil {
		b.subscribers[channelName] = make(map[string]*Subscriber)
	}
	b.subscribers[channelName][subscriberID] = sub

	log.Printf("[pubsub] subscriber %s joined channel %s", shortID(subscriberID), channelName)
	return sub
}

func (b *Broker) Unsubscribe(channelName, subscriberID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.channels[channelName]
	if !ok {
		return
	}

	ch.mu.Lock()
	if sub, ok := ch.subscribers[subscriberID]; ok {
		sub.mu.Lock()
		if !sub.closed {
			close(sub.Ch)
			sub.closed = true
		}
		sub.mu.Unlock()
		delete(ch.subscribers, subscriberID)
	}
	ch.mu.Unlock()

	delete(b.subscribers[channelName], subscriberID)
	log.Printf("[pubsub] subscriber %s left channel %s", shortID(subscriberID), channelName)
}

func (b *Broker) Publish(channelName string, data []byte) int {
	b.mu.RLock()
	ch, ok := b.channels[channelName]
	b.mu.RUnlock()

	if !ok {
		return 0
	}

	msg := Message{Channel: channelName, Data: data}
	count := 0

	ch.mu.RLock()
	for _, sub := range ch.subscribers {
		sub.mu.Lock()
		if !sub.closed {
			select {
			case sub.Ch <- msg:
				count++
			default:
			}
		}
		sub.mu.Unlock()
	}
	ch.mu.RUnlock()

	return count
}

func (b *Broker) ChannelCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.channels)
}

func (b *Broker) SubscriberCount(channelName string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.channels[channelName]
	if !ok {
		return 0
	}
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.subscribers)
}

func (b *Broker) Channels() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	channels := make([]string, 0, len(b.channels))
	for name := range b.channels {
		channels = append(channels, name)
	}
	return channels
}
