package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// awsBackend implements storage.Interface using aws services.
type awsBackend struct {
	s3            s3API
	sns           *sns.Client
	sqs           *sqs.Client
	groupResource schema.GroupResource
	codec         runtime.Codec
	pathPrefix    string
	bucket        string
}

// s3API is the interface for s3 client.
type s3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	GetObjectTagging(ctx context.Context, input *s3.GetObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	PutObjectTagging(ctx context.Context, input *s3.PutObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Returns Versioner associated with this interface.
func (a *awsBackend) Versioner() storage.Versioner {
	return etcd3.Versioner
}

// Create adds a new object at a key unless it already exists. 'ttl' is time-to-live
// in seconds (0 means forever). If no error is returned and out is not nil, out will be
// set to the read value from database.
func (a *awsBackend) Create(ctx context.Context, key string, obj runtime.Object, out runtime.Object, ttl uint64) error {
	if version, err := a.Versioner().ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion should not be set on objects to be created")
	}
	if err := a.Versioner().PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	data, err := runtime.Encode(a.codec, obj)
	if err != nil {
		return err
	}

	// TODO: support ttl by tagging expiration time for the S3 object.
	key = a.generateKey(key)
	_, err = a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(runtime.ContentTypeJSON),
	})
	if err != nil {
		return err
	}

	// TODO: manage rev number
	if out != nil {
		return decode(a.codec, a.Versioner(), data, out, 1)
	}
	return nil
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

// generateKey generates key for the object.
func (a *awsBackend) generateKey(key string) string {
	return path.Join(a.pathPrefix, key) + ".json"
}

// decode decodes value of bytes into object. It will also set the object resource version to rev.
// On success, objPtr would be set to the object.
func decode(codec runtime.Codec, versioner storage.Versioner, value []byte, objPtr runtime.Object, rev int64) error {
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_, _, err := codec.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	if err := versioner.UpdateObject(objPtr, uint64(rev)); err != nil {
		// klog.Errorf("failed to update object version: %v", err)
	}
	return nil
}

// NewAWSStorage creates a new storage backend based on provided aws config.
func NewAWSStorage(config aws.Config, bucket string, c storagebackend.ConfigForResource, newFunc func() runtime.Object) storage.Interface {
	return &awsBackend{
		s3:            s3.NewFromConfig(config),
		sns:           sns.NewFromConfig(config),
		sqs:           sqs.NewFromConfig(config),
		groupResource: c.GroupResource,
		codec:         c.Codec,
		pathPrefix:    c.Prefix,
		bucket:        bucket,
	}
}

var _ storage.Interface = &awsBackend{}
