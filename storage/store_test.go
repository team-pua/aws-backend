package storage

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
)

var scheme = runtime.NewScheme()
var codecs = serializer.NewCodecFactory(scheme)

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
}

func TestCreate(t *testing.T) {
	ctx, store := testSetup(t)
	store.s3 = &fakeS3{}

	key := "/testkey"
	out := &example.Pod{}
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", SelfLink: "testlink"}}

	err := store.Create(ctx, key, obj, out, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// basic tests of the output
	if obj.ObjectMeta.Name != out.ObjectMeta.Name {
		t.Errorf("pod name want=%s, get=%s", obj.ObjectMeta.Name, out.ObjectMeta.Name)
	}
	if out.ResourceVersion == "" {
		t.Errorf("output should have non-empty resource version")
	}
	if out.SelfLink != "" {
		t.Errorf("output should have empty self link")
	}
}

func testSetup(t *testing.T) (context.Context, *awsBackend) {
	codec := apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)
	store := &awsBackend{
		codec: codec,
		groupResource: schema.GroupResource{
			Resource: "pods",
		},
	}
	ctx := context.Background()
	return ctx, store
}

type fakeS3 struct {
	putObjOutput        *s3.PutObjectOutput
	getObjOutput        *s3.GetObjectOutput
	listObjOutput       *s3.ListObjectVersionsOutput
	getObjTaggingOutput *s3.GetObjectTaggingOutput
	putObjTaggingOutput *s3.PutObjectTaggingOutput
	deleteObjOutput     *s3.DeleteObjectOutput

	putObjErr        error
	getObjErr        error
	listObjErr       error
	getObjTaggingErr error
	putObjTaggingErr error
	deleteObjErr     error
}

func (f *fakeS3) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.putObjOutput, f.putObjErr
}

func (f *fakeS3) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f.getObjOutput, f.getObjErr
}

func (f *fakeS3) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	return f.listObjOutput, f.listObjErr
}

func (f *fakeS3) GetObjectTagging(ctx context.Context, input *s3.GetObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	return f.getObjTaggingOutput, f.getObjTaggingErr
}

func (f *fakeS3) PutObjectTagging(ctx context.Context, input *s3.PutObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error) {
	return f.putObjTaggingOutput, f.putObjTaggingErr
}

func (f *fakeS3) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return f.deleteObjOutput, f.deleteObjErr
}

var _ s3API = &fakeS3{}
