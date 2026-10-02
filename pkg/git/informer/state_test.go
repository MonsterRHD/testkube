package informer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// memoryBaselineStore is a test BaselineStore that keeps baselines in memory,
// optionally failing Load/Save to simulate an unavailable backend.
type memoryBaselineStore struct {
	mu        sync.Mutex
	baselines map[string]string
	loadErr   error
	saveErr   error
	saves     int
}

func newMemoryBaselineStore() *memoryBaselineStore {
	return &memoryBaselineStore{baselines: make(map[string]string)}
}

func (s *memoryBaselineStore) Load(context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	result := make(map[string]string, len(s.baselines))
	for k, v := range s.baselines {
		result[k] = v
	}
	return result, nil
}

func (s *memoryBaselineStore) Save(_ context.Context, baselines map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.baselines = make(map[string]string, len(baselines))
	for k, v := range baselines {
		s.baselines[k] = v
	}
	return nil
}

func (s *memoryBaselineStore) setSaveErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveErr = err
}

func (s *memoryBaselineStore) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]string, len(s.baselines))
	for k, v := range s.baselines {
		result[k] = v
	}
	return result
}

func (s *memoryBaselineStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func TestConfigMapBaselineStore_LoadMissingReturnsEmpty(t *testing.T) {
	store := NewConfigMapBaselineStore(fake.NewSimpleClientset(), "testkube", "")

	baselines, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Empty(t, baselines)
}

func TestConfigMapBaselineStore_SaveCreatesAndUpdates(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	store := NewConfigMapBaselineStore(clientset, "testkube", "custom-state")

	require.NoError(t, store.Save(context.Background(), map[string]string{
		"v1:ns/a|ref|refs/heads/main": "hash-1",
		"v1:ns/b|ref|pr:1":            "sha-1:open",
	}))

	configMap, err := clientset.CoreV1().ConfigMaps("testkube").Get(context.Background(), "custom-state", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, configMap.Data, baselineConfigMapDataKey)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"v1:ns/a|ref|refs/heads/main": "hash-1",
		"v1:ns/b|ref|pr:1":            "sha-1:open",
	}, loaded)

	// Second save updates the existing ConfigMap.
	require.NoError(t, store.Save(context.Background(), map[string]string{
		"v1:ns/a|ref|refs/heads/main": "hash-2",
	}))
	loaded, err = store.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"v1:ns/a|ref|refs/heads/main": "hash-2"}, loaded)
}

func TestConfigMapBaselineStore_SaveAdoptsExistingConfigMap(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testkube", Name: DefaultBaselineConfigMapName},
		Data:       map[string]string{"unrelated": "keep-me"},
	})
	store := NewConfigMapBaselineStore(clientset, "testkube", DefaultBaselineConfigMapName)

	require.NoError(t, store.Save(context.Background(), map[string]string{"k": "v"}))

	configMap, err := clientset.CoreV1().ConfigMaps("testkube").Get(context.Background(), DefaultBaselineConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "keep-me", configMap.Data["unrelated"], "unrelated ConfigMap entries must be preserved")
	assert.NotEmpty(t, configMap.Data[baselineConfigMapDataKey])
}

func TestConfigMapBaselineStore_LoadCorruptReturnsError(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testkube", Name: DefaultBaselineConfigMapName},
		Data:       map[string]string{baselineConfigMapDataKey: "not-json"},
	})
	store := NewConfigMapBaselineStore(clientset, "testkube", DefaultBaselineConfigMapName)

	_, err := store.Load(context.Background())
	require.Error(t, err)
}

func TestFileBaselineStore_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "baselines.json")
	store := NewFileBaselineStore(path)

	// Missing file is not an error.
	baselines, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Empty(t, baselines)

	require.NoError(t, store.Save(context.Background(), map[string]string{
		"v1:ns/a|ref|refs/heads/main": "hash-1",
	}))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	baselines, err = store.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"v1:ns/a|ref|refs/heads/main": "hash-1"}, baselines)
}

func TestUnmarshalBaselines_RejectsUnknownVersion(t *testing.T) {
	_, err := unmarshalBaselines([]byte(`{"version":99,"commits":{}}`))
	require.Error(t, err)
}

func TestBaselineEnvelopeRoundTrip(t *testing.T) {
	payload, err := marshalBaselines(map[string]string{"k": "v"})
	require.NoError(t, err)
	loaded, err := unmarshalBaselines(payload)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, loaded)
}

var _ BaselineStore = (*memoryBaselineStore)(nil)

var errBaselineUnavailable = errors.New("baseline backend unavailable")
