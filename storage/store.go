package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// awsBackend implements storage.Interface using aws services.
type awsBackend struct {
	s3  *s3.Client
	sns *sns.Client
	sqs *sqs.Client
}

// Returns Versioner associated with this interface.
func (a *awsBackend) Versioner() storage.Versioner {
	return etcd3.Versioner
}

// Create adds a new object at a key unless it already exists. 'ttl' is time-to-live
// in seconds (0 means forever). If no error is returned and out is not nil, out will be
// set to the read value from database.
func (a *awsBackend) Create(ctx context.Context, key string, obj runtime.Object, out runtime.Object, ttl uint64) error {
	panic("not implemented") // TODO: Implement
}

// Delete removes the specified key and returns the value that existed at that spot.
// If key didn't exist, it will return NotFound storage error.
// If 'cachedExistingObject' is non-nil, it can be used as a suggestion about the
// current version of the object to avoid read operation from storage to get it.
// However, the implementations have to retry in case suggestion is stale.
func (a *awsBackend) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object) error {
	panic("not implemented") // TODO: Implement
}

// Watch begins watching the specified key. Events are decoded into API objects,
// and any items selected by 'p' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
// If resource version is "0", this interface will get current object at given key
// and send it in an "ADDED" event, before watch starts.
func (a *awsBackend) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	panic("not implemented") // TODO: Implement
}

// WatchList begins watching the specified key's items. Items are decoded into API
// objects and any item selected by 'p' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
// If resource version is "0", this interface will list current objects directory defined by key
// and send them in "ADDED" events, before watch starts.
func (a *awsBackend) WatchList(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	panic("not implemented") // TODO: Implement
}

// Get unmarshals json found at key into objPtr. On a not found error, will either
// return a zero object of the requested type, or an error, depending on 'opts.ignoreNotFound'.
// Treats empty responses and nil response nodes exactly like a not found error.
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	panic("not implemented") // TODO: Implement
}

// GetToList unmarshals json found at key and opaque it into *List api object
// (an object that satisfies the runtime.IsList definition).
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) GetToList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	panic("not implemented") // TODO: Implement
}

// List unmarshalls jsons found at directory defined by key and opaque them
// into *List api object (an object that satisfies runtime.IsList definition).
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) List(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	panic("not implemented") // TODO: Implement
}

// GuaranteedUpdate keeps calling 'tryUpdate()' to update key 'key' (of type 'ptrToType')
// retrying the update until success if there is index conflict.
// Note that object passed to tryUpdate may change across invocations of tryUpdate() if
// other writers are simultaneously updating it, so tryUpdate() needs to take into account
// the current contents of the object when deciding how the update object should look.
// If the key doesn't exist, it will return NotFound storage error if ignoreNotFound=false
// or zero value in 'ptrToType' parameter otherwise.
// If the object to update has the same value as previous, it won't do any update
// but will return the object in 'ptrToType' parameter.
// If 'cachedExistingObject' is non-nil, it can be used as a suggestion about the
// current version of the object to avoid read operation from storage to get it.
// However, the implementations have to retry in case suggestion is stale.
//
// Example:
//
// s := /* implementation of Interface */
// err := s.GuaranteedUpdate(
//     "myKey", &MyType{}, true,
//     func(input runtime.Object, res ResponseMeta) (runtime.Object, *uint64, error) {
//       // Before each invocation of the user defined function, "input" is reset to
//       // current contents for "myKey" in database.
//       curr := input.(*MyType)  // Guaranteed to succeed.
//
//       // Make the modification
//       curr.Counter++
//
//       // Return the modified object - return an error to stop iterating. Return
//       // a uint64 to alter the TTL on the object, or nil to keep it the same value.
//       return cur, nil, nil
//    },
// )
func (a *awsBackend) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	panic("not implemented") // TODO: Implement
}

// Count returns number of different entries under the key (generally being path prefix).
func (a *awsBackend) Count(key string) (int64, error) {
	panic("not implemented") // TODO: Implement
}

// NewAWSStorage creates a new storage backend based on provided aws config.
func NewAWSStorage(config aws.Config, c storagebackend.Config, newFunc func() runtime.Object) storage.Interface {
	return &awsBackend{
		s3:  s3.NewFromConfig(config),
		sns: sns.NewFromConfig(config),
		sqs: sqs.NewFromConfig(config),
	}
}

var _ storage.Interface = &awsBackend{}
