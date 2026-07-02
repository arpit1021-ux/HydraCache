package pubsub

import (
	"testing"
	"time"
)

func TestBrokerPublishSubscribe(t *testing.T) {
	broker := NewBroker()

	sub := broker.Subscribe("test-channel", "sub-1")
	defer broker.Unsubscribe("test-channel", "sub-1")

	count := broker.Publish("test-channel", []byte("hello"))
	if count != 1 {
		t.Errorf("expected 1 subscriber, got %d", count)
	}

	select {
	case msg := <-sub.Ch:
		if string(msg.Data) != "hello" {
			t.Errorf("expected 'hello', got '%s'", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for message")
	}
}

func TestBrokerMultipleSubscribers(t *testing.T) {
	broker := NewBroker()

	sub1 := broker.Subscribe("ch1", "s1")
	sub2 := broker.Subscribe("ch1", "s2")
	defer broker.Unsubscribe("ch1", "s1")
	defer broker.Unsubscribe("ch1", "s2")

	broker.Publish("ch1", []byte("msg1"))

	<-sub1.Ch
	<-sub2.Ch
}

func TestBrokerUnsubscribe(t *testing.T) {
	broker := NewBroker()

	broker.Subscribe("ch1", "s1")
	broker.Unsubscribe("ch1", "s1")

	if broker.SubscriberCount("ch1") != 0 {
		t.Error("expected 0 subscribers after unsubscribe")
	}
}

func TestBrokerChannelCount(t *testing.T) {
	broker := NewBroker()

	broker.Subscribe("ch1", "s1")
	broker.Subscribe("ch2", "s2")
	broker.Subscribe("ch3", "s3")

	if broker.ChannelCount() != 3 {
		t.Errorf("expected 3 channels, got %d", broker.ChannelCount())
	}
}

func TestBrokerPublishNoSubscribers(t *testing.T) {
	broker := NewBroker()

	count := broker.Publish("empty-channel", []byte("data"))
	if count != 0 {
		t.Errorf("expected 0 for no subscribers, got %d", count)
	}
}
