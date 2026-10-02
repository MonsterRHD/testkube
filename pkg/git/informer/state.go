package informer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultBaselineConfigMapName is the ConfigMap used to persist processed
	// git state (last seen commit hashes and PR states) so that restarting the
	// API server or the informer does not replay already handled events.
	DefaultBaselineConfigMapName = "testkube-git-informer-state"

	baselineConfigMapDataKey = "baselines.json"
	baselineStateVersion     = 1

	// baselineSaveRetries bounds read-modify-write retries when the ConfigMap
	// changes concurrently (for example on a leader handover).
	baselineSaveRetries = 3
)

// BaselineStore persists the informer's processed-state baselines (last seen
// per-ref commit hashes and pull-request states) across process restarts.
//
// Save receives a complete snapshot of the current baseline; implementations
// are expected to replace their stored content atomically.
type BaselineStore interface {
	// Load returns the persisted baselines. A missing backing object is not an
	// error and yields an empty map.
	Load(ctx context.Context) (map[string]string, error)
	// Save replaces the persisted baselines with the given snapshot.
	Save(ctx context.Context, baselines map[string]string) error
}

// persistedBaselines is the on-disk/on-wire envelope for the baseline map.
type persistedBaselines struct {
	Version int               `json:"version"`
	Commits map[string]string `json:"commits"`
}

func marshalBaselines(baselines map[string]string) ([]byte, error) {
	return json.Marshal(persistedBaselines{
		Version: baselineStateVersion,
		Commits: baselines,
	})
}

func unmarshalBaselines(data []byte) (map[string]string, error) {
	var state persistedBaselines
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing persisted git baselines: %w", err)
	}
	if state.Version != baselineStateVersion {
		return nil, fmt.Errorf("unsupported persisted git baseline version %d (want %d)", state.Version, baselineStateVersion)
	}
	if state.Commits == nil {
		return map[string]string{}, nil
	}
	return state.Commits, nil
}

// configMapBaselineStore keeps the baseline snapshot in a single ConfigMap
// data entry. The informer is leader-gated, so there is at most one writer;
// concurrent updates are retried as a safety net.
type configMapBaselineStore struct {
	client    kubernetes.Interface
	namespace string
	name      string
}

// NewConfigMapBaselineStore returns a BaselineStore backed by a ConfigMap in
// the given namespace. An empty name defaults to DefaultBaselineConfigMapName.
func NewConfigMapBaselineStore(client kubernetes.Interface, namespace, name string) BaselineStore {
	if name == "" {
		name = DefaultBaselineConfigMapName
	}
	return &configMapBaselineStore{
		client:    client,
		namespace: namespace,
		name:      name,
	}
}

func (s *configMapBaselineStore) Load(ctx context.Context) (map[string]string, error) {
	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading git baseline ConfigMap %s/%s: %w", s.namespace, s.name, err)
	}
	raw, ok := configMap.Data[baselineConfigMapDataKey]
	if !ok || raw == "" {
		return map[string]string{}, nil
	}
	return unmarshalBaselines([]byte(raw))
}

func (s *configMapBaselineStore) Save(ctx context.Context, baselines map[string]string) error {
	payload, err := marshalBaselines(baselines)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < baselineSaveRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		configMap, getErr := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			_, createErr := s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      s.name,
					Namespace: s.namespace,
					Labels:    map[string]string{"app.kubernetes.io/managed-by": "testkube-git-informer"},
				},
				Data: map[string]string{baselineConfigMapDataKey: string(payload)},
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(createErr) {
				continue // lost the create race; re-read and update
			}
			if createErr != nil {
				return fmt.Errorf("creating git baseline ConfigMap %s/%s: %w", s.namespace, s.name, createErr)
			}
			return nil
		}
		if getErr != nil {
			return fmt.Errorf("reading git baseline ConfigMap %s/%s: %w", s.namespace, s.name, getErr)
		}

		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		configMap.Data[baselineConfigMapDataKey] = string(payload)
		_, updateErr := s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			continue
		}
		if updateErr != nil {
			return fmt.Errorf("updating git baseline ConfigMap %s/%s: %w", s.namespace, s.name, updateErr)
		}
		return nil
	}

	return fmt.Errorf("could not persist git baselines to ConfigMap %s/%s after %d attempts", s.namespace, s.name, baselineSaveRetries)
}

// fileBaselineStore keeps the baseline snapshot in a JSON file. Writes are
// atomic (temporary file in the same directory followed by a rename), so a
// crashed or interrupted write never leaves a partially written baseline.
type fileBaselineStore struct {
	path string
}

// NewFileBaselineStore returns a BaselineStore backed by a JSON file at path.
// It is intended for deployments without a Kubernetes API available and for
// tests.
func NewFileBaselineStore(path string) BaselineStore {
	return &fileBaselineStore{path: path}
}

func (s *fileBaselineStore) Load(_ context.Context) (map[string]string, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading git baseline file %s: %w", s.path, err)
	}
	return unmarshalBaselines(data)
}

func (s *fileBaselineStore) Save(ctx context.Context, baselines map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := marshalBaselines(baselines)
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating git baseline directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".git-baselines-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temporary git baseline file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return fmt.Errorf("writing git baseline file %s: %w", s.path, err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("setting git baseline file permissions for %s: %w", s.path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing git baseline file %s: %w", s.path, err)
	}

	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("renaming temporary git baseline file to %s: %w", s.path, err)
	}
	return nil
}
