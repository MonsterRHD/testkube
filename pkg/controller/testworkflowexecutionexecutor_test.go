package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	commonv1 "github.com/kubeshop/testkube/api/common/v1"
	testworkflowsv1 "github.com/kubeshop/testkube/api/testworkflows/v1"
	testkubev1 "github.com/kubeshop/testkube/pkg/api/v1/testkube"
	"github.com/kubeshop/testkube/pkg/cloud"
)

var scheduleCmpOpts = []cmp.Option{
	cmpopts.IgnoreUnexported(
		cloud.ScheduleRequest{},
		cloud.RunningContext{},
		cloud.ScheduleExecution{},
		cloud.ScheduleResourceSelector{},
		cloud.ExecutionTarget{},
		cloud.ExecutionTargetLabels{},
	),
}

type fakeExecuteResponse struct {
	results []testkubev1.TestWorkflowExecution
	err     error
}

type fakeTestWorkflowExecutor struct {
	// static response used by the table tests
	results []testkubev1.TestWorkflowExecution
	err     error
	// scripted responses used by the multi-reconcile tests; the last one is
	// repeated for any further call
	responses []fakeExecuteResponse
	calls     int
	req       *cloud.ScheduleRequest
}

type fakeEventRecorder struct {
	Events chan string
}

func newFakeEventRecorder(buffer int) *fakeEventRecorder {
	return &fakeEventRecorder{Events: make(chan string, buffer)}
}

func (r *fakeEventRecorder) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, _ string, note string, args ...any) {
	r.Events <- fmt.Sprintf("%s %s %s", eventtype, reason, fmt.Sprintf(note, args...))
}

func (f *fakeTestWorkflowExecutor) Execute(_ context.Context, request *cloud.ScheduleRequest) ([]testkubev1.TestWorkflowExecution, error) {
	f.req = request
	call := f.calls
	f.calls++
	if len(f.responses) > 0 {
		if call >= len(f.responses) {
			call = len(f.responses) - 1
		}
		return f.responses[call].results, f.responses[call].err
	}
	return f.results, f.err
}

func TestWorkflowExecutionExecutorController(t *testing.T) {
	tests := map[string]struct {
		objs      []client.Object
		request   reconcile.Request
		expect    *cloud.ScheduleRequest
		execErr   error
		wantError string
		wantEvent string
	}{
		// Should pass through a basic test workflow execution.
		"execute": {
			objs: []client.Object{
				&testworkflowsv1.TestWorkflowExecution{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestWorkflowExecution",
						APIVersion: "testworkflows.testkube.io/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-workflow-execution",
						Namespace:  "test-namespace",
						Generation: 1,
					},
					Spec: testworkflowsv1.TestWorkflowExecutionSpec{
						TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
					},
					Status: testworkflowsv1.TestWorkflowExecutionStatus{},
				},
			},
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: &cloud.ScheduleRequest{
				Executions: []*cloud.ScheduleExecution{{
					Selector: &cloud.ScheduleResourceSelector{Name: "test-workflow"},
				}},
				RunningContext: &cloud.RunningContext{
					Name: "test-workflow-execution",
					Type: cloud.RunningContextType_KUBERNETESOBJECT,
				},
			},
			wantEvent: "Normal ExecutionScheduled Scheduled test workflow \"test-workflow\"",
		},
		// Should pass through a basic test workflow execution with target selectors.
		"target": {
			objs: []client.Object{
				&testworkflowsv1.TestWorkflowExecution{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestWorkflowExecution",
						APIVersion: "testworkflows.testkube.io/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-workflow-execution",
						Namespace:  "test-namespace",
						Generation: 1,
					},
					Spec: testworkflowsv1.TestWorkflowExecutionSpec{
						TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
						ExecutionRequest: &testworkflowsv1.TestWorkflowExecutionRequest{
							Target: &commonv1.Target{
								SchedulerPolicy: commonv1.SchedulerPolicyOnlyWhenMatches,
								Match: map[string][]string{
									"foo": {"bar"},
									"baz": {"qux", "quux"},
								},
								Not: map[string][]string{
									"one":   {"two"},
									"three": {"four", "five"},
								},
							},
						},
					},
					Status: testworkflowsv1.TestWorkflowExecutionStatus{},
				},
			},
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: &cloud.ScheduleRequest{
				Executions: []*cloud.ScheduleExecution{{
					Selector: &cloud.ScheduleResourceSelector{Name: "test-workflow"},
					Targets: []*cloud.ExecutionTarget{{
						SchedulerPolicy: string(commonv1.SchedulerPolicyOnlyWhenMatches),
						Match: map[string]*cloud.ExecutionTargetLabels{
							"foo": {Labels: []string{"bar"}},
							"baz": {Labels: []string{"qux", "quux"}},
						},
						Not: map[string]*cloud.ExecutionTargetLabels{
							"one":   {Labels: []string{"two"}},
							"three": {Labels: []string{"four", "five"}},
						},
					}},
				}},
				RunningContext: &cloud.RunningContext{
					Name: "test-workflow-execution",
					Type: cloud.RunningContextType_KUBERNETESOBJECT,
				},
			},
			wantEvent: "Normal ExecutionScheduled Scheduled test workflow \"test-workflow\"",
		},
		// Should not execute if the generation has not changed.
		"ignore generation": {
			objs: []client.Object{
				&testworkflowsv1.TestWorkflowExecution{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestWorkflowExecution",
						APIVersion: "testworkflows.testkube.io/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-workflow-execution",
						Namespace:  "test-namespace",
						Generation: 1,
					},
					Spec: testworkflowsv1.TestWorkflowExecutionSpec{
						TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
						ExecutionRequest: &testworkflowsv1.TestWorkflowExecutionRequest{
							Target: &commonv1.Target{
								Match: map[string][]string{
									"foo": {"bar"},
									"baz": {"qux", "quux"},
								},
								Not: map[string][]string{
									"one":   {"two"},
									"three": {"four", "five"},
								},
							},
						},
					},
					Status: testworkflowsv1.TestWorkflowExecutionStatus{
						Generation: 1,
					},
				},
			},
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: nil,
		},
		// Should do nothing when the resource was deleted.
		"not found": {
			objs: nil,
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: nil,
		},
		// Should update status with error and emit a Warning event on execution failure.
		"execution error updates status": {
			objs: []client.Object{
				&testworkflowsv1.TestWorkflowExecution{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestWorkflowExecution",
						APIVersion: "testworkflows.testkube.io/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-workflow-execution",
						Namespace:  "test-namespace",
						Generation: 1,
					},
					Spec: testworkflowsv1.TestWorkflowExecutionSpec{
						TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
					},
					Status: testworkflowsv1.TestWorkflowExecutionStatus{},
				},
			},
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: &cloud.ScheduleRequest{
				Executions: []*cloud.ScheduleExecution{{
					Selector: &cloud.ScheduleResourceSelector{Name: "test-workflow"},
				}},
				RunningContext: &cloud.RunningContext{
					Name: "test-workflow-execution",
					Type: cloud.RunningContextType_KUBERNETESOBJECT,
				},
			},
			execErr:   fmt.Errorf("dial tcp: lookup testkube-api-server on 192.168.0.1:53: no such host"),
			wantError: "dial tcp: lookup testkube-api-server on 192.168.0.1:53: no such host",
			wantEvent: "Warning ExecutionNotScheduled dial tcp: lookup testkube-api-server on 192.168.0.1:53: no such host",
		},
		// Should clear a previous error on successful execution.
		"success clears previous error": {
			objs: []client.Object{
				&testworkflowsv1.TestWorkflowExecution{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestWorkflowExecution",
						APIVersion: "testworkflows.testkube.io/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-workflow-execution",
						Namespace:  "test-namespace",
						Generation: 2,
					},
					Spec: testworkflowsv1.TestWorkflowExecutionSpec{
						TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
					},
					Status: testworkflowsv1.TestWorkflowExecutionStatus{
						Generation: 1,
						Error:      "previous error from failed attempt",
					},
				},
			},
			request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-workflow-execution",
					Namespace: "test-namespace",
				},
			},
			expect: &cloud.ScheduleRequest{
				Executions: []*cloud.ScheduleExecution{{
					Selector: &cloud.ScheduleResourceSelector{Name: "test-workflow"},
				}},
				RunningContext: &cloud.RunningContext{
					Name: "test-workflow-execution",
					Type: cloud.RunningContextType_KUBERNETESOBJECT,
				},
			},
			wantEvent: "Normal ExecutionScheduled Scheduled test workflow \"test-workflow\"",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exec := &fakeTestWorkflowExecutor{err: test.execErr}
			scheme := runtime.NewScheme()
			if err := testworkflowsv1.AddToScheme(scheme); err != nil {
				t.Fatalf("failed to add testworkflowsv1 to scheme: %v", err)
			}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.objs...).WithStatusSubresource(&testworkflowsv1.TestWorkflowExecution{}).Build()
			recorder := newFakeEventRecorder(10)
			reconciler := testWorkflowExecutionExecutor(k8sClient, recorder, exec)

			_, err := reconciler.Reconcile(context.Background(), test.request)
			if err != nil {
				t.Errorf("reconcile: %v", err)
			}
			if !cmp.Equal(test.expect, exec.req, scheduleCmpOpts...) {
				t.Errorf("Incorrect execution request, diff: %s", cmp.Diff(test.expect, exec.req, scheduleCmpOpts...))
			}

			// After a successful execution, verify that Status.Generation was updated
			// to match Generation so the deduplication guard prevents re-execution,
			// and that any previous error has been cleared.
			if exec.req != nil && test.wantError == "" {
				var twe testworkflowsv1.TestWorkflowExecution
				if err := k8sClient.Get(context.Background(), test.request.NamespacedName, &twe); err != nil {
					t.Fatalf("get TestWorkflowExecution: %v", err)
				}
				if twe.Status.Generation != twe.Generation {
					t.Errorf("Status.Generation = %d, want %d", twe.Status.Generation, twe.Generation)
				}
				if twe.Status.Error != "" {
					t.Errorf("Status.Error = %q, want empty", twe.Status.Error)
				}
			}

			// On error, verify status was updated with the error message and generation.
			if test.wantError != "" {
				var twe testworkflowsv1.TestWorkflowExecution
				if err := k8sClient.Get(context.Background(), test.request.NamespacedName, &twe); err != nil {
					t.Fatalf("get TestWorkflowExecution: %v", err)
				}
				if twe.Status.Error != test.wantError {
					t.Errorf("Status.Error = %q, want %q", twe.Status.Error, test.wantError)
				}
				if twe.Status.Generation != twe.Generation {
					t.Errorf("Status.Generation = %d, want %d", twe.Status.Generation, twe.Generation)
				}
			}

			// Verify the expected event was emitted.
			if test.wantEvent != "" {
				select {
				case got := <-recorder.Events:
					if got != test.wantEvent {
						t.Errorf("event = %q, want %q", got, test.wantEvent)
					}
				case <-time.After(time.Second):
					t.Error("expected an event to be recorded, but none was")
				}
			}
		})
	}

}

const (
	testExecutionObjectName = "test-workflow-execution"
	testExecutionNamespace  = "test-namespace"
	scheduledEvent          = "Normal ExecutionScheduled Scheduled test workflow \"test-workflow\""
)

func newExecutionRequestResource(gen int64, status testworkflowsv1.TestWorkflowExecutionStatus) *testworkflowsv1.TestWorkflowExecution {
	return &testworkflowsv1.TestWorkflowExecution{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestWorkflowExecution",
			APIVersion: "testworkflows.testkube.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       testExecutionObjectName,
			Namespace:  testExecutionNamespace,
			Generation: gen,
		},
		Spec: testworkflowsv1.TestWorkflowExecutionSpec{
			TestWorkflow: &v1.LocalObjectReference{Name: "test-workflow"},
		},
		Status: status,
	}
}

func newReconcilerTestSetup(t *testing.T, obj *testworkflowsv1.TestWorkflowExecution, exec *fakeTestWorkflowExecutor) (client.Client, reconcile.Reconciler, *fakeEventRecorder, reconcile.Request) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := testworkflowsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add testworkflowsv1 to scheme: %v", err)
	}
	objs := []client.Object{}
	if obj != nil {
		objs = append(objs, obj)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&testworkflowsv1.TestWorkflowExecution{}).Build()
	recorder := newFakeEventRecorder(10)
	return k8sClient, testWorkflowExecutionExecutor(k8sClient, recorder, exec), recorder, reconcile.Request{
		NamespacedName: client.ObjectKey{Name: testExecutionObjectName, Namespace: testExecutionNamespace},
	}
}

func apiExecution(id, name string, status *testkubev1.TestWorkflowStatus, initErr string) testkubev1.TestWorkflowExecution {
	execution := testkubev1.TestWorkflowExecution{
		Id:          id,
		Name:        name,
		Namespace:   "runner-namespace",
		Number:      42,
		ScheduledAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		Result:      &testkubev1.TestWorkflowResult{Status: status},
	}
	if initErr != "" {
		execution.Result.Initialization = &testkubev1.TestWorkflowStepResult{ErrorMessage: initErr}
	}
	return execution
}

func requireEvent(t *testing.T, recorder *fakeEventRecorder, want string) {
	t.Helper()
	select {
	case got := <-recorder.Events:
		if !strings.HasPrefix(got, want) {
			t.Errorf("event = %q, want prefix %q", got, want)
		}
	case <-time.After(time.Second):
		t.Errorf("expected event %q, but none was recorded", want)
	}
}

func requireNoEvent(t *testing.T, recorder *fakeEventRecorder) {
	t.Helper()
	select {
	case got := <-recorder.Events:
		t.Errorf("expected no event, but got %q", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func findObservedResult(t *testing.T, twe *testworkflowsv1.TestWorkflowExecution, id string) testworkflowsv1.TestWorkflowExecutionObservedResult {
	t.Helper()
	for _, execution := range twe.Status.Executions {
		if execution.Id == id {
			return execution
		}
	}
	t.Fatalf("execution %q not found in status entries %v", id, twe.Status.Executions)
	return testworkflowsv1.TestWorkflowExecutionObservedResult{}
}

func getExecutionResource(t *testing.T, k8sClient client.Client) *testworkflowsv1.TestWorkflowExecution {
	t.Helper()
	var twe testworkflowsv1.TestWorkflowExecution
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: testExecutionObjectName, Namespace: testExecutionNamespace}, &twe); err != nil {
		t.Fatalf("get TestWorkflowExecution: %v", err)
	}
	return &twe
}

// Multiple related executions returned for one request are synced item by
// item: each name, error and completion status lands in resource status.
func TestReconcileSyncsMultipleResults(t *testing.T) {
	queued := testkubev1.QUEUED_TestWorkflowStatus
	failed := testkubev1.FAILED_TestWorkflowStatus
	passed := testkubev1.PASSED_TestWorkflowStatus
	exec := &fakeTestWorkflowExecutor{results: []testkubev1.TestWorkflowExecution{
		apiExecution("exec-a", "workflow-1", &queued, ""),
		apiExecution("exec-b", "workflow-2", &failed, "pod unschedulable"),
		apiExecution("exec-c", "workflow-3", &passed, ""),
	}}
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, newExecutionRequestResource(1, testworkflowsv1.TestWorkflowExecutionStatus{}), exec)

	result, err := reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %s, want 0", result.RequeueAfter)
	}
	if exec.calls != 1 {
		t.Errorf("executor calls = %d, want 1", exec.calls)
	}

	twe := getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 1 {
		t.Errorf("Status.Generation = %d, want 1", twe.Status.Generation)
	}
	if twe.Status.ExecutionsGeneration != 1 {
		t.Errorf("Status.ExecutionsGeneration = %d, want 1", twe.Status.ExecutionsGeneration)
	}
	if twe.Status.Error != "" {
		t.Errorf("Status.Error = %q, want empty", twe.Status.Error)
	}
	if len(twe.Status.Executions) != 3 {
		t.Fatalf("Status.Executions len = %d, want 3", len(twe.Status.Executions))
	}
	// Order follows the returned results.
	wantIds := []string{"exec-a", "exec-b", "exec-c"}
	for i, wantId := range wantIds {
		if twe.Status.Executions[i].Id != wantId {
			t.Errorf("Status.Executions[%d].Id = %q, want %q", i, twe.Status.Executions[i].Id, wantId)
		}
	}
	b := findObservedResult(t, twe, "exec-b")
	if b.Name != "workflow-2" {
		t.Errorf("exec-b Name = %q, want workflow-2", b.Name)
	}
	if b.Status == nil || *b.Status != testworkflowsv1.FAILED_TestWorkflowStatus {
		t.Errorf("exec-b Status = %v, want failed", b.Status)
	}
	if b.Error != "pod unschedulable" {
		t.Errorf("exec-b Error = %q, want pod unschedulable", b.Error)
	}
	if b.ScheduledAt.IsZero() {
		t.Error("exec-b ScheduledAt not synced")
	}
	c := findObservedResult(t, twe, "exec-c")
	if c.Status == nil || *c.Status != testworkflowsv1.PASSED_TestWorkflowStatus {
		t.Errorf("exec-c Status = %v, want passed", c.Status)
	}
	if c.Error != "" {
		t.Errorf("exec-c Error = %q, want empty", c.Error)
	}
	requireEvent(t, recorder, scheduledEvent)
}

// A stream that confirms some results and then fails keeps the confirmed
// entries, reports the unfinished part and is retried without advancing the
// generation; a failed retry keeps what was confirmed, and the eventual
// success completes the same request without duplicating synced results.
func TestReconcileRetriesPartialResultsAndResumes(t *testing.T) {
	queued := testkubev1.QUEUED_TestWorkflowStatus
	exec := &fakeTestWorkflowExecutor{responses: []fakeExecuteResponse{
		{results: []testkubev1.TestWorkflowExecution{
			apiExecution("exec-a", "workflow-1", &queued, ""),
			apiExecution("exec-b", "workflow-2", &queued, ""),
		}, err: fmt.Errorf("stream interrupted")},
		{err: fmt.Errorf("still unavailable")},
		{results: []testkubev1.TestWorkflowExecution{
			apiExecution("exec-a", "workflow-1", &queued, ""),
			apiExecution("exec-b", "workflow-2", &queued, ""),
			apiExecution("exec-c", "workflow-3", &queued, ""),
		}},
	}}
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, newExecutionRequestResource(1, testworkflowsv1.TestWorkflowExecutionStatus{}), exec)

	// First reconcile: two executions confirmed, then the stream failed.
	result, err := reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if result.RequeueAfter != partialScheduleRequeueDelay {
		t.Errorf("RequeueAfter = %s, want %s", result.RequeueAfter, partialScheduleRequeueDelay)
	}
	twe := getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 0 {
		t.Errorf("Status.Generation = %d, want 0 while unfinished", twe.Status.Generation)
	}
	if twe.Status.ExecutionsGeneration != 1 {
		t.Errorf("Status.ExecutionsGeneration = %d, want 1", twe.Status.ExecutionsGeneration)
	}
	if len(twe.Status.Executions) != 2 {
		t.Fatalf("confirmed entries = %d, want 2", len(twe.Status.Executions))
	}
	if !strings.Contains(twe.Status.Error, "2 related execution(s) confirmed") || !strings.Contains(twe.Status.Error, "stream interrupted") {
		t.Errorf("Status.Error = %q, want it to report confirmed count and cause", twe.Status.Error)
	}
	requireEvent(t, recorder, "Warning ExecutionPartiallyScheduled")

	// Second reconcile: executor still failing with no new results; the
	// previously confirmed results must be preserved.
	result, err = reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if result.RequeueAfter != partialScheduleRequeueDelay {
		t.Errorf("RequeueAfter = %s, want %s", result.RequeueAfter, partialScheduleRequeueDelay)
	}
	twe = getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 0 {
		t.Errorf("Status.Generation = %d, want 0 while unfinished", twe.Status.Generation)
	}
	if len(twe.Status.Executions) != 2 {
		t.Fatalf("confirmed entries = %d, want 2 preserved", len(twe.Status.Executions))
	}
	if !strings.Contains(twe.Status.Error, "still unavailable") {
		t.Errorf("Status.Error = %q, want the latest cause", twe.Status.Error)
	}

	// Third reconcile: the remaining result arrives. Already synced results
	// must not be recreated.
	result, err = reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("third reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %s, want 0", result.RequeueAfter)
	}
	if exec.calls != 3 {
		t.Errorf("executor calls = %d, want 3", exec.calls)
	}
	twe = getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 1 {
		t.Errorf("Status.Generation = %d, want 1", twe.Status.Generation)
	}
	if twe.Status.Error != "" {
		t.Errorf("Status.Error = %q, want empty", twe.Status.Error)
	}
	if len(twe.Status.Executions) != 3 {
		t.Fatalf("Status.Executions len = %d, want 3", len(twe.Status.Executions))
	}
	seen := map[string]int{}
	for _, execution := range twe.Status.Executions {
		seen[execution.Id]++
	}
	for _, id := range []string{"exec-a", "exec-b", "exec-c"} {
		if seen[id] != 1 {
			t.Errorf("execution %q synced %d times, want once", id, seen[id])
		}
	}
	requireEvent(t, recorder, "Warning ExecutionPartiallyScheduled")
	requireEvent(t, recorder, scheduledEvent)
}

// After a controller restart the partially synced resource status is the
// resume point: results already present are not created again when the
// executor returns them while completing the request.
func TestReconcileResumesAfterRestartWithoutDuplicates(t *testing.T) {
	queued := testkubev1.QUEUED_TestWorkflowStatus
	kubeQueued := testworkflowsv1.QUEUED_TestWorkflowStatus
	exec := &fakeTestWorkflowExecutor{results: []testkubev1.TestWorkflowExecution{
		apiExecution("exec-a", "workflow-1", &queued, ""),
		apiExecution("exec-b", "workflow-2", &queued, ""),
	}}
	restarted := newExecutionRequestResource(1, testworkflowsv1.TestWorkflowExecutionStatus{
		Executions: []testworkflowsv1.TestWorkflowExecutionObservedResult{{
			Id:     "exec-a",
			Name:   "workflow-1",
			Status: &kubeQueued,
		}},
		ExecutionsGeneration: 1,
		Error:                "partially scheduled: 1 related execution(s) confirmed, executor failed before all results were returned: interrupted",
	})
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, restarted, exec)

	result, err := reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %s, want 0", result.RequeueAfter)
	}
	if exec.calls != 1 {
		t.Errorf("executor calls = %d, want 1", exec.calls)
	}
	twe := getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 1 {
		t.Errorf("Status.Generation = %d, want 1", twe.Status.Generation)
	}
	if twe.Status.Error != "" {
		t.Errorf("Status.Error = %q, want empty", twe.Status.Error)
	}
	if len(twe.Status.Executions) != 2 {
		t.Fatalf("Status.Executions len = %d, want 2", len(twe.Status.Executions))
	}
	if twe.Status.Executions[0].Id != "exec-a" || twe.Status.Executions[1].Id != "exec-b" {
		t.Errorf("Status.Executions ids = %v, want [exec-a exec-b]", twe.Status.Executions)
	}
	requireEvent(t, recorder, scheduledEvent)
}

// Observations from a previous, fully synced generation are replaced by the
// batch for the new generation rather than merged across requests.
func TestReconcileNewGenerationReplacesObservedResults(t *testing.T) {
	queued := testkubev1.QUEUED_TestWorkflowStatus
	kubePassed := testworkflowsv1.PASSED_TestWorkflowStatus
	exec := &fakeTestWorkflowExecutor{results: []testkubev1.TestWorkflowExecution{
		apiExecution("exec-new", "workflow-new", &queued, ""),
	}}
	resource := newExecutionRequestResource(2, testworkflowsv1.TestWorkflowExecutionStatus{
		Executions: []testworkflowsv1.TestWorkflowExecutionObservedResult{{
			Id:     "exec-old",
			Name:   "workflow-old",
			Status: &kubePassed,
		}},
		ExecutionsGeneration: 1,
		Generation:           1,
	})
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, resource, exec)

	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	twe := getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 2 || twe.Status.ExecutionsGeneration != 2 {
		t.Errorf("generations = %d/%d, want 2/2", twe.Status.Generation, twe.Status.ExecutionsGeneration)
	}
	if len(twe.Status.Executions) != 1 || twe.Status.Executions[0].Id != "exec-new" {
		t.Errorf("Status.Executions = %v, want only exec-new", twe.Status.Executions)
	}
	requireEvent(t, recorder, scheduledEvent)
}

// Deleting a resource must not call the executor.
func TestReconcileDeletedResourceDoesNotExecute(t *testing.T) {
	exec := &fakeTestWorkflowExecutor{}
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, nil, exec)

	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if exec.calls != 0 {
		t.Errorf("executor calls = %d, want 0", exec.calls)
	}
	if err := k8sClient.Get(context.Background(), req.NamespacedName, &testworkflowsv1.TestWorkflowExecution{}); !errors.IsNotFound(err) {
		t.Errorf("expected not found, got %v", err)
	}
	requireNoEvent(t, recorder)
}

// An executor returning no results keeps the existing behavior: generation
// advances, error stays empty.
func TestReconcileEmptyResultsKeepsExistingBehavior(t *testing.T) {
	exec := &fakeTestWorkflowExecutor{}
	k8sClient, reconciler, recorder, req := newReconcilerTestSetup(t, newExecutionRequestResource(1, testworkflowsv1.TestWorkflowExecutionStatus{}), exec)

	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	twe := getExecutionResource(t, k8sClient)
	if twe.Status.Generation != 1 {
		t.Errorf("Status.Generation = %d, want 1", twe.Status.Generation)
	}
	if twe.Status.Error != "" {
		t.Errorf("Status.Error = %q, want empty", twe.Status.Error)
	}
	if len(twe.Status.Executions) != 0 {
		t.Errorf("Status.Executions len = %d, want 0", len(twe.Status.Executions))
	}
	requireEvent(t, recorder, scheduledEvent)
}
