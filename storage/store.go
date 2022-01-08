package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	txn           Txn
}

type authenticatedDataString string

// s3API is the interface for s3 client.
type s3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjects(ctx context.Context, input *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
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
	v, err := conversion.EnforcePtr(out)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	key = a.generateKey(key)
	return a.conditionalDelete(ctx, key, out, v, preconditions, validateDeletion, cachedExistingObject)
}

func (a *awsBackend) conditionalDelete(
	ctx context.Context, key string, out runtime.Object, v reflect.Value, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object) error {
	getCurrentState := func() (*ObjectState, error) {
		getResp, revision, err := a.getObjectAndRevision(ctx, key)
		if err != nil {
			return nil, err
		}
		return a.getState(getResp, key, v, false, revision)
	}

	var (
		origState          *ObjectState
		origStateIsCurrent bool
		err                error
	)
	if cachedExistingObject != nil {
		origState, err = a.getStateFromObject(cachedExistingObject)
	} else {
		origState, err = getCurrentState()
		origStateIsCurrent = true
	}
	if err != nil {
		return err
	}

	for {
		origObject := getRuntimeObj(v, origState.Content)
		if preconditions != nil {
			if err := preconditions.Check(key, origObject); err != nil {
				if origStateIsCurrent {
					return err
				}

				// It's possible we're working with stale data.
				// Remember the revision of the potentially stale data and the resulting update error
				cachedRev := origState.Revision
				cachedUpdateErr := err

				// Actually fetch
				origState, err = getCurrentState()
				if err != nil {
					return err
				}
				origStateIsCurrent = true

				// it turns out our cached data was not stale, return the error
				if cachedRev == origState.Revision {
					return cachedUpdateErr
				}

				// Retry
				continue
			}
		}
		if err := validateDeletion(ctx, origObject); err != nil {
			if origStateIsCurrent {
				return err
			}

			// It's possible we're working with stale data.
			// Remember the revision of the potentially stale data and the resulting update error
			cachedRev := origState.Revision
			cachedUpdateErr := err

			// Actually fetch
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true

			// it turns out our cached data was not stale, return the error
			if cachedRev == origState.Revision {
				return cachedUpdateErr
			}

			// Retry
			continue
		}

		origState, err = a.txn.Delete(ctx, key, origState.Revision)
		if err != nil {
			return err
		}
		if len(origState.Content) == 0 {
			origStateIsCurrent = true
			continue
		}
		return decode(a.codec, a.Versioner(), origState.Content, out, int64(origState.Revision))
	}

	return nil
}

func getRuntimeObj(v reflect.Value, data []byte) runtime.Object {
	if u, ok := v.Addr().Interface().(runtime.Unstructured); ok {
		return u.NewEmptyInstance()
	}
	return reflect.New(v.Type()).Interface().(runtime.Object)
}

func (a *awsBackend) getStateFromObject(obj runtime.Object) (*ObjectState, error) {

	return nil, nil
}

func (a *awsBackend) getObjectAndRevision(ctx context.Context, key string) ([]byte, uint64, error) {
	listOut, err := a.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(a.bucket),
		Prefix: &key,
	})
	if err != nil {
		return nil, 0, err
	}

	latestCommitted, err := getLatestCommittedVersion(ctx, a.s3, a.bucket, key, listOut)
	if err != nil {
		return nil, 0, storage.NewKeyNotFoundError(key, 0)
	}

	// not found
	if latestCommitted == nil { // no version
		return nil, 0, nil
	}

	data, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(a.bucket),
		Key:       aws.String(key),
		VersionId: latestCommitted.VersionId,
	})
	if err != nil {
		return nil, 0, err
	}

	raw, err := ioutil.ReadAll(data.Body)
	if err != nil {
		return nil, 0, err
	}

	tagSet, err := getObjectTagSet(ctx, a.s3, a.bucket, key, *latestCommitted.VersionId)
	if err != nil {
		return nil, 0, err
	}

	latestRevision := getRevision(tagSet)

	return raw, latestRevision, nil
}

func (a *awsBackend) getState(getResp []byte, key string, v reflect.Value, ignoreNotFound bool, revision uint64) (*ObjectState, error) {
	state := &ObjectState{}
	var obj runtime.Object
	if u, ok := v.Addr().Interface().(runtime.Unstructured); ok {
		obj = u.NewEmptyInstance()
	} else {
		obj = reflect.New(v.Type()).Interface().(runtime.Object)
	}

	if len(getResp) == 0 {
		if !ignoreNotFound {
			return nil, storage.NewKeyNotFoundError(key, 0)
		}
		if err := runtime.SetZeroValue(obj); err != nil {
			return nil, err
		}
		data, err := runtime.Encode(a.codec, obj)
		if err != nil {
			return nil, err
		}
		state.Content = data
	} else {
		state.Content = getResp
		state.Revision = revision
	}

	return state, nil
}

// Watch begins watching the specified key. Events are decoded into API objects,
// and any items selected by 'p' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
// If resource version is "0", this interface will get current object at given key
// and send it in an "ADDED" event, before watch starts.
func (a *awsBackend) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	// TODO: implement watch
	return &dummyWatcher{}, nil
}

// WatchList begins watching the specified key's items. Items are decoded into API
// objects and any item selected by 'p' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
// If resource version is "0", this interface will list current objects directory defined by key
// and send them in "ADDED" events, before watch starts.
func (a *awsBackend) WatchList(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	// TODO: implement watch
	return &dummyWatcher{}, nil
}

// Get unmarshals json found at key into objPtr. On a not found error, will either
// return a zero object of the requested type, or an error, depending on 'opts.ignoreNotFound'.
// Treats empty responses and nil response nodes exactly like a not found error.
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	key = a.generateKey(key)
	listOut, err := a.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(a.bucket),
		Prefix: &key,
	})
	if err != nil {
		return err
	}

	latestCommitted, err := getLatestCommittedVersion(ctx, a.s3, a.bucket, key, listOut)
	if err != nil || latestCommitted == nil {
		return storage.NewKeyNotFoundError(key, 0)
	}

	tagSet, err := getObjectTagSet(ctx, a.s3, a.bucket, key, *latestCommitted.VersionId)
	if err != nil {
		return err
	}

	rev := getRevision(tagSet)
	data, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(a.bucket),
		Key:       aws.String(key),
		VersionId: latestCommitted.VersionId,
	})
	if err != nil {
		return err
	}
	raw, err := ioutil.ReadAll(data.Body)
	if err != nil {
		return err
	}
	return decode(a.codec, a.Versioner(), raw, objPtr, int64(rev))
}

// GetToList unmarshals json found at key and opaque it into *List api object
// (an object that satisfies the runtime.IsList definition).
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) GetToList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return a.list(ctx, key, opts, listObj, false)
}

// List unmarshalls jsons found at directory defined by key and opaque them
// into *List api object (an object that satisfies runtime.IsList definition).
// The returned contents may be delayed, but it is guaranteed that they will
// match 'opts.ResourceVersion' according 'opts.ResourceVersionMatch'.
func (a *awsBackend) List(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return a.list(ctx, key, opts, listObj, true)
}

func (a *awsBackend) list(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object, recursive bool) error {
	pred := opts.Predicate
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}
	key = path.Join(a.pathPrefix, key)

	// For recursive lists, we need to make sure the key ended with "/" so that we only
	// get children "directories". e.g. if we have key "/a", "/a/b", "/ab", getting keys
	// with prefix "/a" will return all three, while with prefix "/a/" will return only
	// "/a/b" which is the correct answer.
	if recursive && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	keyPrefix := key
	newItemFunc := getNewItemFunc(listObj, v)
	var returnedRV int64

	// loop until we have filled the requested limit from etcd or there are no more results
	var numFetched int
	var numEvald int
	for {
		objListOut, err := a.s3.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(a.bucket),
			Prefix: aws.String(keyPrefix),
		})
		if err != nil {
			return err
		}

		numFetched += len(objListOut.Contents)

		// take items from the response until the bucket is full, filtering as we go
		for _, s3Obj := range objListOut.Contents {
			obj, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(a.bucket),
				Key:    s3Obj.Key,
			})
			if err != nil {
				return err
			}
			data, err := ioutil.ReadAll(obj.Body)
			if err != nil {
				return err
			}

			listVerOut, err := a.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
				Bucket: &a.bucket,
				Prefix: &key,
			})
			if err != nil {
				return err
			}
			latestCommitted, err := getLatestCommittedVersion(ctx, a.s3, a.bucket, *s3Obj.Key, listVerOut)
			if err != nil {
				return err
			}
			tagSet, err := getObjectTagSet(ctx, a.s3, a.bucket, *s3Obj.Key, *latestCommitted.VersionId)
			if err != nil {
				return err
			}

			if err := appendListItem(v, data, getRevision(tagSet), pred, a.codec, a.Versioner(), newItemFunc); err != nil {
				return err
			}
			numEvald++
		}

		// indicate to the client which resource version was returned
		if returnedRV == 0 {
			returnedRV = int64(genRevision())
		}

		// we're paging but we have filled our bucket
		if int64(v.Len()) >= pred.Limit {
			break
		}
	}

	// no continuation
	return a.Versioner().UpdateList(listObj, uint64(returnedRV), "", nil)
}

func getNewItemFunc(listObj runtime.Object, v reflect.Value) func() runtime.Object {
	// For unstructured lists with a target group/version, preserve the group/version in the instantiated list items
	if unstructuredList, isUnstructured := listObj.(*unstructured.UnstructuredList); isUnstructured {
		if apiVersion := unstructuredList.GetAPIVersion(); len(apiVersion) > 0 {
			return func() runtime.Object {
				return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": apiVersion}}
			}
		}
	}

	// Otherwise just instantiate an empty item
	elem := v.Type().Elem()
	return func() runtime.Object {
		return reflect.New(elem).Interface().(runtime.Object)
	}
}

// appendListItem decodes and appends the object (if it passes filter) to v, which must be a slice.
func appendListItem(v reflect.Value, data []byte, rev uint64, pred storage.SelectionPredicate, codec runtime.Codec, versioner storage.Versioner, newItemFunc func() runtime.Object) error {
	obj, _, err := codec.Decode(data, nil, newItemFunc())
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	if err := versioner.UpdateObject(obj, rev); err != nil {
		// klog.Errorf("failed to update object version: %v", err)
	}
	if matched, err := pred.Matches(obj); err == nil && matched {
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}
	return nil
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
func (a *awsBackend) GuaranteedUpdate(ctx context.Context, key string, out runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	panic("not implemented") // TODO: Implement
	v, err := conversion.EnforcePtr(out)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	key = a.generateKey(key)

	getCurrentState := func() (*ObjectState, error) {
		getResp, revision, err := a.getObjectAndRevision(ctx, key)
		if err != nil {
			return nil, err
		}
		return a.getState(getResp, key, v, false, revision)
	}

	var origState *ObjectState
	var origStateIsCurrent bool
	if cachedExistingObject != nil {
		origState, err = a.getStateFromObject(cachedExistingObject)
	} else {
		origState, err = getCurrentState()
		origStateIsCurrent = true
	}
	if err != nil {
		return err
	}

	for {
		origObject := getRuntimeObj(v, origState.Content)
		if err := preconditions.Check(key, origObject); err != nil {
			// If our data is already up to date, return the error
			if origStateIsCurrent {
				return err
			}

			// It's possible we were working with stale data
			// Actually fetch
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true
			// Retry
			continue
		}

		listOut, err := a.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(a.bucket),
			Prefix: &key,
		})

		latestCommitted, err := getLatestCommittedVersion(ctx, a.s3, a.bucket, key, listOut)
		if err != nil {
			return err
		}

		// not found
		if latestCommitted == nil { // no version
			return storage.NewKeyNotFoundError(key, 0)
		}
		tagSet, err := getObjectTagSet(ctx, a.s3, a.bucket, key, *latestCommitted.VersionId)
		if err != nil {
			return err
		}
		objTTL, err := getTTL(tagSet)
		if err != nil {
			return err
		}
		// TODO: pass ttl to Update transaction method
		ret, _, err := a.updateState(origState, origObject, objTTL, tryUpdate)
		if err != nil {
			// If our data is already up to date, return the error
			if origStateIsCurrent {
				return err
			}

			// It's possible we were working with stale data
			// Remember the revision of the potentially stale data and the resulting update error
			cachedRev := origState.Revision
			cachedUpdateErr := err

			// Actually fetch
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true

			// it turns out our cached data was not stale, return the error
			if cachedRev == origState.Revision {
				return cachedUpdateErr
			}

			// Retry
			continue
		}

		data, err := runtime.Encode(a.codec, ret)
		if err != nil {
			return err
		}

		// newData, err := s.transformer.TransformToStorage(data, transformContext)
		if err != nil {
			return storage.NewInternalError(err.Error())
		}

		origState, err = a.txn.Update(ctx, key, data, origState.Revision)
		if err != nil {
			return err
		}
		if len(origState.Content) == 0 {
			// getResp := (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			// klog.V(4).Infof("GuaranteedUpdate of %s failed because of a conflict, going to retry", key)
			origState, err = a.getState(origState.Content, key, v, ignoreNotFound, origState.Revision)
			if err != nil {
				return err
			}
			origStateIsCurrent = true
			continue
		}

		return decode(a.codec, a.Versioner(), data, out, int64(origState.Revision))
	}
}

func (a *awsBackend) updateState(st *ObjectState, obj runtime.Object, ttl int64, userUpdate storage.UpdateFunc) (runtime.Object, uint64, error) {
	ret, ttlPtr, err := userUpdate(obj, storage.ResponseMeta{TTL: ttl, ResourceVersion: st.Revision})
	if err != nil {
		return nil, 0, err
	}

	if err := a.Versioner().PrepareObjectForStorage(ret); err != nil {
		return nil, 0, fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	if ttlPtr != nil {
		ttl = int64(*ttlPtr)
	}
	return ret, uint64(ttl), nil
}

// Count returns number of different entries under the key (generally being path prefix).
func (a *awsBackend) Count(key string) (int64, error) {
	key = path.Join(a.pathPrefix, key)

	// We need to make sure the key ended with "/" so that we only get children "directories".
	// e.g. if we have key "/a", "/a/b", "/ab", getting keys with prefix "/a" will return all three,
	// while with prefix "/a/" will return only "/a/b" which is the correct answer.
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	listOut, err := a.s3.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(a.bucket),
		Prefix: &key,
	})
	if err != nil {
		return 0, err
	}

	return int64(len(listOut.Contents)), nil
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
		txn: &txn{
			s3:     s3.NewFromConfig(config),
			bucket: bucket,
		},
	}
}

var _ storage.Interface = &awsBackend{}
