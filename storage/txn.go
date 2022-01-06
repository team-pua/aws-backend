package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

const (
	trueVal       = "true"
	falseVal      = "false"
	commitedKey   = "committed"
	expirationKey = "expiration"
	ttlKey        = "ttl"
	revisionKey   = "revision"
)

// Txn is the interface that wraps mini-transactions.
type Txn interface {
	Create(ctx context.Context, key string, value []byte, ttl uint64) error
	Delete(ctx context.Context, key string, origRev uint64) (ObjectState, error)
	Update(ctx context.Context, key string, value []byte, origRev uint64) (state ObjectState, err error)
}

type txn struct {
	s3     s3API
	bucket string
}

type ObjectState struct {
	Content  []byte
	Revision uint64
}

func (t *txn) Create(ctx context.Context, key string, value []byte, ttl uint64) (err error) {
	input := &s3.PutObjectInput{
		Bucket:      &t.bucket,
		Key:         &key,
		Body:        bytes.NewReader(value),
		ContentType: aws.String(runtime.ContentTypeJSON),
	}
	if ttl > 0 {
		tagSet := types.Tagging{}
		tagSet.TagSet = []types.Tag{
			{
				Key:   aws.String(ttlKey),
				Value: aws.String(strconv.FormatUint(ttl, 10)),
			},
			{
				Key:   aws.String(expirationKey),
				Value: aws.String(time.Now().Add(time.Duration(ttl) * time.Second).Format(time.RFC3339)),
			},
		}
		tagSetJSON, err := json.Marshal(tagSet)
		if err != nil {
			return err
		}
		input.Tagging = aws.String(string(tagSetJSON))
	}
	out, err := t.s3.PutObject(ctx, input)
	if err != nil {
		return err
	}
	// ensure the object is not created before
	defer func() {
		if err != nil {
			_, err = t.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    &t.bucket,
				Key:       &key,
				VersionId: out.VersionId,
			})
			if err != nil {
				panic(fmt.Errorf("Create: failed clean up key %s: %v", key, err))
			}
			return
		}
		err = commitObject(ctx, t.s3, t.bucket, key, out.VersionId)
		if err != nil {
			panic(fmt.Errorf("Create: failed commit key %s: %v", key, err))
		}
	}()
	listOut, err := t.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:          &t.bucket,
		Prefix:          &key,
		VersionIdMarker: out.VersionId,
	})
	if err != nil {
		return err
	}

	// check if any existing versions are not deleted
	if getLatestVersion(listOut) != nil {
		return storage.NewKeyExistsError(key, 0)
	}

	return nil
}

func (t *txn) mutateKey(ctx context.Context, key, op string, f func(context.Context) (*string, error), origRev uint64) (state ObjectState, err error) {
	listOut, err := t.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &t.bucket,
		Prefix: &key,
	})
	if err != nil {
		return ObjectState{}, err
	}
	origLatest := getLatestVersion(listOut)
	latestCommitted, err := getLatestCommittedVersion(ctx, t.s3, t.bucket, key, listOut)
	if err != nil {
		return ObjectState{}, err
	}
	if origLatest == nil || latestCommitted == nil {
		return ObjectState{}, storage.NewKeyNotFoundError(key, 0)
	}

	tagSet, err := getObjectTagSet(ctx, t.s3, t.bucket, key, *latestCommitted.VersionId)
	if err != nil {
		return ObjectState{}, err
	}

	if getRevision(tagSet) != origRev {
		latestRevision := getRevision(tagSet)
		if latestRevision < origRev {
			return ObjectState{}, fmt.Errorf("%s: the object's cached revision %v is lower than the latest detected revision %v", op, origRev, latestRevision)
		}
		out, err := t.s3.GetObject(ctx, &s3.GetObjectInput{
			Bucket:    &t.bucket,
			Key:       &key,
			VersionId: latestCommitted.VersionId,
		})
		if err != nil {
			return ObjectState{}, err
		}
		content, err := ioutil.ReadAll(out.Body)
		if err != nil {
			return ObjectState{}, err
		}
		return ObjectState{Content: content, Revision: latestRevision}, nil
	}

	opVersion, err := f(ctx)
	if err != nil {
		return ObjectState{}, err
	}

	// check if need to revert the deletion
	defer func() {
		if err != nil {
			panic(fmt.Errorf("%s: failed checking key %s: %v", op, key, err))
		}
	}()

	// update the versions
	listOut, err = t.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:          &t.bucket,
		Prefix:          &key,
		VersionIdMarker: opVersion,
	})
	if err != nil {
		return ObjectState{}, err
	}
	latestAfterOperation := getLatestVersion(listOut)
	if latestAfterOperation == nil {
		return ObjectState{}, fmt.Errorf("%s: failed to find the latest version after deletion", op)
	}
	if origLatest.VersionId != latestAfterOperation.VersionId {
		// some new version added during deletion, revert the deletion operation
		_, err = t.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    &t.bucket,
			Key:       &key,
			VersionId: opVersion,
		})
		if err != nil {
			return ObjectState{}, err
		}
	}
	return ObjectState{}, nil
}

func (t *txn) Delete(ctx context.Context, key string, origRev uint64) (state ObjectState, err error) {
	return t.mutateKey(ctx, key, "Delete", func(ctx context.Context) (*string, error) {
		out, err := t.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &t.bucket,
			Key:    &key,
		})
		if err != nil {
			return nil, err
		}
		return out.VersionId, nil
	}, origRev)
}

func (t *txn) Update(ctx context.Context, key string, value []byte, origRev uint64) (state ObjectState, err error) {
	return t.mutateKey(ctx, key, "Update", func(ctx context.Context) (*string, error) {
		out, err := t.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &t.bucket,
			Key:    &key,
			Body:   bytes.NewReader(value),
		})
		if err != nil {
			return nil, err
		}
		return out.VersionId, nil
	}, origRev)
}

var _ Txn = &txn{}

// getLatestVersion returns the latest not deleted version given versionIDs and deletion markers.
func getLatestVersion(out *s3.ListObjectVersionsOutput) *types.ObjectVersion {
	if len(out.Versions) == 0 {
		return nil
	}
	if len(out.DeleteMarkers) == 0 {
		return &out.Versions[0]
	}
	if out.Versions[0].LastModified.After(*out.DeleteMarkers[0].LastModified) {
		return &out.Versions[0]
	}
	return nil
}

// getObjectTagSet returns the tagset of the given object with a specific version.
func getObjectTagSet(ctx context.Context, svc s3API, bucket, key, versionID string) ([]types.Tag, error) {
	out, err := svc.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: &versionID,
	})
	if err != nil {
		return nil, err
	}
	return out.TagSet, nil
}

// isExpired checks if the given tagset is expired.
func isExpired(tagSet []types.Tag) (bool, error) {
	for _, tag := range tagSet {
		if *tag.Key != expirationKey {
			continue
		}
		return isTimestampExpired(*tag.Value)
	}
	return false, nil
}

func isTimestampExpired(timestamp string) (bool, error) {
	ts, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return false, err
	}
	return ts.Before(time.Now()), nil
}

// isCommitted checks if the given tagset is committed.
func isCommitted(tagSet []types.Tag) bool {
	for _, tag := range tagSet {
		if *tag.Key != commitedKey {
			continue
		}
		return *tag.Value == trueVal
	}
	return false
}

// getRevision returns the revision of the given object.
func getRevision(tagSet []types.Tag) uint64 {
	for _, tag := range tagSet {
		if *tag.Key != revisionKey {
			continue
		}
		revision, _ := strconv.ParseUint(*tag.Value, 10, 64)
		return revision
	}
	return 0
}

// genRevision generates a revision number.
func genRevision() uint64 {
	ts := time.Now()
	return uint64(ts.UnixNano())
}

// commitObject adds the commit tag to the given object.
func commitObject(ctx context.Context, svc s3API, bucket, key string, versionID *string) error {
	_, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: versionID,
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String(commitedKey),
					Value: aws.String(trueVal),
				},
			},
		},
	})
	return err
}

// getLatestCommittedVersion returns the latest committed version of the given object.
func getLatestCommittedVersion(ctx context.Context, svc s3API, bucket, key string, out *s3.ListObjectVersionsOutput) (*types.ObjectVersion, error) {
	for _, version := range out.Versions {
		if len(out.DeleteMarkers) > 0 && !version.LastModified.After(*out.DeleteMarkers[0].LastModified) {
			return nil, nil
		}
		tagSet, err := getObjectTagSet(ctx, svc, bucket, key, *version.VersionId)
		if err != nil {
			return nil, err
		}
		if isCommitted(tagSet) {
			return &version, nil
		}
	}
	return nil, nil
}
