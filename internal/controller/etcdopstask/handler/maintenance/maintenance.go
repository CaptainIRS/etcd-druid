/*
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: Apache-2.0
*/

package maintenance

import (
	"context"
	"fmt"
	"net/http"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	taskhandler "github.com/gardener/etcd-druid/internal/controller/etcdopstask/handler"
	taskutils "github.com/gardener/etcd-druid/internal/controller/etcdopstask/handler/utils"
	"github.com/gardener/etcd-druid/internal/utils"
	"github.com/gardener/etcd-druid/internal/utils/imagevector"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// compactHandler implements the task.Handler interface for handling maintenance compact tasks.
type compactHandler struct {
	k8sClient   client.Client
	task        *druidv1alpha1.EtcdOpsTask
	imageVector imagevector.ImageVector
}

// defragHandler implements the task.Handler interface for handling maintenance defrag tasks.
type defragHandler struct {
	k8sClient   client.Client
	task        *druidv1alpha1.EtcdOpsTask
	imageVector imagevector.ImageVector
}

// NewCompact creates a new compactHandler instance.
func NewCompact(k8sClient client.Client, task *druidv1alpha1.EtcdOpsTask, _ *http.Client, imageVector imagevector.ImageVector) (taskhandler.Handler, error) {
	return &compactHandler{
		k8sClient:   k8sClient,
		task:        task,
		imageVector: imageVector,
	}, nil
}

// NewDefrag creates a new defragHandler instance.
func NewDefrag(k8sClient client.Client, task *druidv1alpha1.EtcdOpsTask, _ *http.Client, imageVector imagevector.ImageVector) (taskhandler.Handler, error) {
	return &defragHandler{
		k8sClient:   k8sClient,
		task:        task,
		imageVector: imageVector,
	}, nil
}

// Admit checks if the compact task can be admitted for execution.
func (h *compactHandler) Admit(ctx context.Context) taskhandler.Result {
	etcd, errResult := taskutils.GetEtcd(ctx, h.k8sClient, h.task.GetEtcdReference(), druidv1alpha1.LastOperationTypeAdmit)
	if errResult != nil {
		errResult.Requeue = false
		return *errResult
	}
	if !etcd.IsReady() {
		return taskhandler.Result{Description: "Etcd is not ready", Error: fmt.Errorf("etcd is not ready"), Requeue: false}
	}
	if res := ensureTLSSecrets(ctx, h.k8sClient, etcd); res != nil {
		return *res
	}
	return taskhandler.Result{Description: "Admit check passed", Requeue: false}
}

// Execute performs the maintenance compact task.
// Creates or monitors a Job that runs etcd-backup-restore `maintenance compact` against the live cluster.
func (h *compactHandler) Execute(ctx context.Context) taskhandler.Result {
	etcd, errResult := taskutils.GetEtcd(ctx, h.k8sClient, h.task.GetEtcdReference(), druidv1alpha1.LastOperationTypeExecution)
	if errResult != nil {
		return *errResult
	}
	etcdBackupRestoreImage, err := utils.GetEtcdBackupRestoreImage(h.imageVector)
	if err != nil || etcdBackupRestoreImage == nil || *etcdBackupRestoreImage == "" {
		return taskhandler.Result{
			Description: "Failed to resolve etcd-backup-restore image",
			Error:       fmt.Errorf("could not resolve etcd-backup-restore image"),
			Requeue:     true,
		}
	}
	args := buildMaintenanceArgs(etcd, "compact", nil)
	activeDeadline := int64(h.task.Spec.Config.Maintenance.Compact.ActiveDeadlineSeconds)
	return ensureMaintenanceJob(ctx, h.k8sClient, h.task, etcd, *etcdBackupRestoreImage, "maintenance-compact", args, activeDeadline)
}

// Cleanup performs any necessary cleanup after the compact task is completed.
func (h *compactHandler) Cleanup(_ context.Context) taskhandler.Result {
	return taskhandler.Result{
		Description: "Cleanup completed",
		Requeue:     false,
	}
}

// Admit checks if the defrag task can be admitted for execution.
func (h *defragHandler) Admit(ctx context.Context) taskhandler.Result {
	etcd, errResult := taskutils.GetEtcd(ctx, h.k8sClient, h.task.GetEtcdReference(), druidv1alpha1.LastOperationTypeAdmit)
	if errResult != nil {
		errResult.Requeue = false
		return *errResult
	}
	if !etcd.IsReady() {
		return taskhandler.Result{Description: "Etcd is not ready", Error: fmt.Errorf("etcd is not ready"), Requeue: false}
	}
	if res := ensureTLSSecrets(ctx, h.k8sClient, etcd); res != nil {
		return *res
	}
	return taskhandler.Result{Description: "Admit check passed", Requeue: false}
}

// Execute performs the maintenance defrag task.
// Creates or monitors a Job that runs etcd-backup-restore `maintenance defrag` against the live cluster.
func (h *defragHandler) Execute(ctx context.Context) taskhandler.Result {
	etcd, errResult := taskutils.GetEtcd(ctx, h.k8sClient, h.task.GetEtcdReference(), druidv1alpha1.LastOperationTypeExecution)
	if errResult != nil {
		return *errResult
	}
	etcdBackupRestoreImage, err := utils.GetEtcdBackupRestoreImage(h.imageVector)
	if err != nil || etcdBackupRestoreImage == nil || *etcdBackupRestoreImage == "" {
		return taskhandler.Result{
			Description: "Failed to resolve etcd-backup-restore image",
			Error:       fmt.Errorf("could not resolve etcd-backup-restore image"),
			Requeue:     true,
		}
	}
	defragTimeout := h.task.Spec.Config.Maintenance.Defrag.TimeoutSeconds
	args := buildMaintenanceArgs(etcd, "defrag", &defragTimeout)
	activeDeadline := int64(h.task.Spec.Config.Maintenance.Defrag.ActiveDeadlineSeconds)
	return ensureMaintenanceJob(ctx, h.k8sClient, h.task, etcd, *etcdBackupRestoreImage, "maintenance-defrag", args, activeDeadline)
}

// Cleanup performs any necessary cleanup after the defrag task is completed.
func (h *defragHandler) Cleanup(_ context.Context) taskhandler.Result {
	return taskhandler.Result{
		Description: "Cleanup completed",
		Requeue:     false,
	}
}
