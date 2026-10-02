package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	testworkflowsv1 "github.com/kubeshop/testkube/api/testworkflows/v1"
	testkubev1 "github.com/kubeshop/testkube/pkg/api/v1/testkube"
	"github.com/kubeshop/testkube/pkg/cloud"
	"github.com/kubeshop/testkube/pkg/mapper/testworkflows"
	"github.com/kubeshop/testkube/pkg/testworkflows/testworkflowexecutor"
)

// partialScheduleRequeueDelay is how long the controller waits before
// re-requesting a schedule whose stream only confirmed part of its results.
const partialScheduleRequeueDelay = 30 * time.Second

type TestWorkflowExecutor interface {
	Execute(ctx context.Context, request *cloud.ScheduleRequest) ([]testkubev1.TestWorkflowExecution, error)
}

func NewTestWorkflowExecutionExecutorController(mgr ctrl.Manager, exec TestWorkflowExecutor) error {
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&testworkflowsv1.TestWorkflowExecution{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(testWorkflowExecutionExecutor(mgr.GetClient(), mgr.GetEventRecorder("testworkflowexecution-controller"), exec)); err != nil {
		return fmt.Errorf("create new controller for TestWorkflowExecution: %w", err)
	}
	return nil
}

func testWorkflowExecutionExecutor(client client.Client, recorder events.EventRecorder, exec TestWorkflowExecutor) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
		// Get and validate the TestWorkflowExecution.
		var twe testworkflowsv1.TestWorkflowExecution
		err := client.Get(ctx, req.NamespacedName, &twe)
		switch {
		case errors.IsNotFound(err):
			return ctrl.Result{}, nil
		case err != nil:
			return ctrl.Result{}, err
		case twe.Spec.TestWorkflow == nil:
			return ctrl.Result{}, nil
		case twe.Generation == twe.Status.Generation:
			return ctrl.Result{}, nil
		case twe.Spec.ExecutionRequest == nil:
			twe.Spec.ExecutionRequest = &testworkflowsv1.TestWorkflowExecutionRequest{}
		}

		// Wrangle the Kubernetes type into the internal representation used by the executor.
		interface_ := testkubev1.API_TestWorkflowRunningContextInterfaceType
		actor := testkubev1.TESTWORKFLOWEXECUTION_TestWorkflowRunningContextActorType
		runningContext := &testkubev1.TestWorkflowRunningContext{
			Interface_: &testkubev1.TestWorkflowRunningContextInterface{
				Type_: &interface_,
			},
			Actor: &testkubev1.TestWorkflowRunningContextActor{
				Name:  twe.Name,
				Type_: &actor,
			},
		}
		rc, user := testworkflowexecutor.GetNewRunningContext(runningContext, nil)

		var scheduleExecution cloud.ScheduleExecution
		if twe.Spec.ExecutionRequest.Target != nil {
			target := &cloud.ExecutionTarget{
				Replicate:       twe.Spec.ExecutionRequest.Target.Replicate,
				SchedulerPolicy: string(twe.Spec.ExecutionRequest.Target.SchedulerPolicy),
			}
			if twe.Spec.ExecutionRequest.Target.Match != nil {
				target.Match = make(map[string]*cloud.ExecutionTargetLabels)
				for k, v := range twe.Spec.ExecutionRequest.Target.Match {
					target.Match[k] = &cloud.ExecutionTargetLabels{Labels: v}
				}
			}
			if twe.Spec.ExecutionRequest.Target.Not != nil {
				target.Not = make(map[string]*cloud.ExecutionTargetLabels)
				for k, v := range twe.Spec.ExecutionRequest.Target.Not {
					target.Not[k] = &cloud.ExecutionTargetLabels{Labels: v}
				}
			}
			scheduleExecution.Targets = []*cloud.ExecutionTarget{target}
		}
		scheduleExecution.Selector = &cloud.ScheduleResourceSelector{Name: twe.Spec.TestWorkflow.Name}
		scheduleExecution.Config = testworkflows.MapConfigValueKubeToAPI(twe.Spec.ExecutionRequest.Config)

		results, err := exec.Execute(ctx, &cloud.ScheduleRequest{
			Executions:           []*cloud.ScheduleExecution{&scheduleExecution},
			DisableWebhooks:      twe.Spec.ExecutionRequest.DisableWebhooks,
			Tags:                 twe.Spec.ExecutionRequest.Tags,
			RunningContext:       rc,
			KubernetesObjectName: twe.Spec.ExecutionRequest.TestWorkflowExecutionName,
			User:                 user,
		})

		// Reload the resource before touching status so the update does not
		// overwrite status written by an event between the Get and now.
		var statusTWE testworkflowsv1.TestWorkflowExecution
		if getErr := client.Get(ctx, req.NamespacedName, &statusTWE); getErr != nil {
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("getting fresh execution %q for error status update: %w (original error: %w)", twe.Name, getErr, err)
			}
			return ctrl.Result{}, fmt.Errorf("getting fresh execution %q for status update: %w", twe.Name, getErr)
		}

		// Results belong to one request generation. A new generation starts a
		// fresh batch; entries for the current generation are kept so that a
		// retry (requeue after partial confirmation or controller restart)
		// continues the same request from what is already in resource status.
		if statusTWE.Status.ExecutionsGeneration != twe.Generation {
			statusTWE.Status.Executions = nil
			statusTWE.Status.ExecutionsGeneration = twe.Generation
		}
		statusTWE.Status.Executions = mergeObservedExecutions(statusTWE.Status.Executions, results)

		if err != nil && len(statusTWE.Status.Executions) == 0 {
			// Nothing at all was confirmed: record the failure and pin the
			// generation to stop the request being retried forever.
			statusTWE.Status.Generation = twe.Generation
			statusTWE.Status.Error = err.Error()
			if updateErr := client.Status().Update(ctx, &statusTWE); updateErr != nil {
				execErr := fmt.Errorf("executing test workflow from execution %q: %w", twe.Name, err)
				return ctrl.Result{}, fmt.Errorf("updating error status for execution %q: %w (original error: %w)", twe.Name, updateErr, execErr)
			}
			recorder.Eventf(&statusTWE, nil, corev1.EventTypeWarning, "ExecutionNotScheduled", "ScheduleExecution", "%s", err.Error())
			return ctrl.Result{}, nil
		}

		if err != nil {
			// Part of the request was confirmed. Keep those results, report
			// the unfinished part and retry the same generation without
			// advancing Status.Generation.
			statusTWE.Status.Error = fmt.Sprintf(
				"partially scheduled: %d related execution(s) confirmed, executor failed before all results were returned: %s",
				len(statusTWE.Status.Executions), err.Error(),
			)
			if updateErr := client.Status().Update(ctx, &statusTWE); updateErr != nil {
				return ctrl.Result{}, fmt.Errorf("updating partial status for execution %q: %w", twe.Name, updateErr)
			}
			recorder.Eventf(&statusTWE, nil, corev1.EventTypeWarning, "ExecutionPartiallyScheduled", "ScheduleExecution", "%s", statusTWE.Status.Error)
			return ctrl.Result{RequeueAfter: partialScheduleRequeueDelay}, nil
		}

		// Every returned result was synced (zero results is a valid outcome):
		// pin the generation to prevent re-execution on operator restart and
		// clear any error left by an earlier partial attempt.
		statusTWE.Status.Generation = twe.Generation
		statusTWE.Status.Error = ""
		if err := client.Status().Update(ctx, &statusTWE); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status generation for execution %q: %w", twe.Name, err)
		}

		recorder.Eventf(&statusTWE, nil, corev1.EventTypeNormal, "ExecutionScheduled", "ScheduleExecution", "Scheduled test workflow %q", twe.Spec.TestWorkflow.Name)

		log := ctrl.LoggerFrom(ctx)
		log.Info("executed test workflow", "name", twe.Spec.TestWorkflow.Name, "executions", len(results))

		return ctrl.Result{}, nil
	})
}

// mergeObservedExecutions syncs freshly returned executions into the status
// entries one item at a time. Entries are keyed by execution id: a result
// already synced is refreshed in place rather than recreated, and new results
// (including ones first seen on a retry after a restart) are appended.
func mergeObservedExecutions(existing []testworkflowsv1.TestWorkflowExecutionObservedResult, results []testkubev1.TestWorkflowExecution) []testworkflowsv1.TestWorkflowExecutionObservedResult {
	index := make(map[string]int, len(existing))
	for i, execution := range existing {
		if execution.Id != "" {
			index[execution.Id] = i
		}
	}
	for _, result := range results {
		observed := testworkflows.MapTestWorkflowObservedResultAPIToKube(result)
		if i, ok := index[observed.Id]; ok && observed.Id != "" {
			existing[i] = observed
			continue
		}
		if observed.Id != "" {
			index[observed.Id] = len(existing)
		}
		existing = append(existing, observed)
	}
	return existing
}
