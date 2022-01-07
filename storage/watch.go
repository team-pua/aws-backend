package storage

import (
	"k8s.io/apimachinery/pkg/watch"
)

type dummyWatcher struct {
	client interface{}
}

// Stops watching. Will close the channel returned by ResultChan(). Releases
// any resources used by the watch.
func (d *dummyWatcher) Stop() {
	return
}

// Returns a chan which will receive all the events. If an error occurs
// or Stop() is called, the implementation will close this channel and
// release any resources used by the watch.
func (d *dummyWatcher) ResultChan() <-chan watch.Event {
	return make(chan watch.Event)
}

var _ watch.Interface = &dummyWatcher{}
