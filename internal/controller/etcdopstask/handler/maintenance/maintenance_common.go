/*
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: Apache-2.0
*/

package maintenance

import (
	"context"
	"fmt"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/etcd-druid/internal/common"
	taskhandler "github.com/gardener/etcd-druid/internal/controller/etcdopstask/handler"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ensureTLSSecrets verifies the presence of TLS CA and client cert secrets for client-url TLS.
// It is intended to be used from the Admit phase only. All errors in Admit should fail fast,
// hence Requeue is always set to false.
func ensureTLSSecrets(ctx context.Context, cl client.Client, etcd *druidv1alpha1.Etcd) *taskhandler.Result {
	tls := etcd.Spec.Etcd.ClientUrlTLS
	if tls == nil {
		return nil
	}

	// Verify CA secret exists
	caSecret := &corev1.Secret{}
	if err := cl.Get(ctx, apitypes.NamespacedName{Namespace: etcd.Namespace, Name: tls.TLSCASecretRef.Name}, caSecret); err != nil {
		return &taskhandler.Result{
			Description: "Failed to get etcd client CA secret",
			Error:       err,
			Requeue:     false,
		}
	}

	// Client TLS secret name must be configured
	if tls.ClientTLSSecretRef.Name == "" {
		return &taskhandler.Result{
			Description: "Client TLS secret not configured",
			Error:       fmt.Errorf("client TLS secret not configured"),
			Requeue:     false,
		}
	}

	// Verify client TLS secret exists
	clientSecret := &corev1.Secret{}
	if err := cl.Get(ctx, apitypes.NamespacedName{Namespace: etcd.Namespace, Name: tls.ClientTLSSecretRef.Name}, clientSecret); err != nil {
		return &taskhandler.Result{
			Description: "Failed to get etcd client TLS secret",
			Error:       err,
			Requeue:     false,
		}
	}

	return nil
}

// buildMaintenanceArgs returns the etcd-backup-restore CLI arguments for a given maintenance operation.
// op must be "compact" or "defrag". If defragTimeoutSeconds != nil, a timeout flag is appended for defrag.
func buildMaintenanceArgs(etcd *druidv1alpha1.Etcd, op string, defragTimeoutSeconds *int32) []string {
	args := []string{"maintenance", op}

	// Add defrag timeout override if provided
	if defragTimeoutSeconds != nil {
		args = append(args, fmt.Sprintf("--etcd-defrag-timeout=%ds", *defragTimeoutSeconds))
	}

	clientPort := ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient)
	if etcd.Spec.Etcd.ClientUrlTLS != nil {
		dataKey := ptr.Deref(etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.DataKey, "ca.crt")
		args = append(args,
			fmt.Sprintf("--cacert=%s/%s", common.VolumeMountPathEtcdCA, dataKey),
			fmt.Sprintf("--cert=%s/tls.crt", common.VolumeMountPathEtcdClientTLS),
			fmt.Sprintf("--key=%s/tls.key", common.VolumeMountPathEtcdClientTLS),
			"--insecure-transport=false",
			"--insecure-skip-tls-verify=false",
			fmt.Sprintf("--endpoints=https://%s-local:%d", etcd.Name, clientPort),
		)
		if druidv1alpha1.IsEtcdRuntimeComponentCreationEnabled(etcd.ObjectMeta) {
			args = append(args, fmt.Sprintf("--service-endpoints=https://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), clientPort))
		}
	} else {
		args = append(args,
			"--insecure-transport=true",
			"--insecure-skip-tls-verify=true",
			fmt.Sprintf("--endpoints=http://%s-local:%d", etcd.Name, clientPort),
		)
		if druidv1alpha1.IsEtcdRuntimeComponentCreationEnabled(etcd.ObjectMeta) {
			args = append(args, fmt.Sprintf("--service-endpoints=http://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), clientPort))
		}
	}

	return args
}

// buildTLSVolumesAndMounts returns volumes and volume mounts for client-url TLS if enabled on the Etcd spec.
// If TLS is not enabled, both returned slices are nil.
func buildTLSVolumesAndMounts(etcd *druidv1alpha1.Etcd) ([]corev1.Volume, []corev1.VolumeMount) {
	if etcd.Spec.Etcd.ClientUrlTLS == nil {
		return nil, nil
	}

	tls := etcd.Spec.Etcd.ClientUrlTLS
	vms := []corev1.VolumeMount{
		{
			Name:      common.VolumeNameEtcdCA,
			MountPath: common.VolumeMountPathEtcdCA,
		},
		{
			Name:      common.VolumeNameEtcdClientTLS,
			MountPath: common.VolumeMountPathEtcdClientTLS,
		},
	}

	vols := []corev1.Volume{
		{
			Name: common.VolumeNameEtcdCA,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tls.TLSCASecretRef.Name},
			},
		},
		{
			Name: common.VolumeNameEtcdClientTLS,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tls.ClientTLSSecretRef.Name},
			},
		},
	}
	return vols, vms
}

// ensureMaintenanceJob is an idempotent helper that inspects or creates a Job for the given maintenance task.
// - If the Job exists and succeeded: returns success with no requeue.
// - If the Job exists and failed: returns error with requeue.
// - If the Job exists and is active: returns in-progress with requeue.
// - If the Job does not exist: creates the Job and returns requeue to observe it later.
//
// The created Job:
// - Is named after the EtcdOpsTask (task.Name).
// - Uses the provided image and container args.
// - Sets ActiveDeadlineSeconds on both Job and Pod.
// - Uses the Etcd ServiceAccount.
// - Mounts TLS secrets only when enabled in the Etcd spec.
// - Sets ownerReference to the EtcdOpsTask to leverage Kubernetes GC after TTL.
func ensureMaintenanceJob(
	ctx context.Context,
	cl client.Client,
	task *druidv1alpha1.EtcdOpsTask,
	etcd *druidv1alpha1.Etcd,
	image string,
	containerName string,
	args []string,
	activeDeadlineSeconds int64,
) taskhandler.Result {
	// Check if job exists
	existing := &batchv1.Job{}
	if err := cl.Get(ctx, apitypes.NamespacedName{Namespace: task.Namespace, Name: task.Name}, existing); err == nil {
		// Inspect status
		if existing.Status.Succeeded > 0 {
			return taskhandler.Result{
				Description: fmt.Sprintf("Maintenance %s job succeeded", containerName),
				Requeue:     false,
			}
		}
		if existing.Status.Failed > 0 {
			return taskhandler.Result{
				Description: fmt.Sprintf("Maintenance %s job failed", containerName),
				Error:       fmt.Errorf("maintenance %s job failed", containerName),
				Requeue:     true,
			}
		}
		// Still running
		return taskhandler.Result{
			Description: fmt.Sprintf("Maintenance %s job in progress", containerName),
			Requeue:     true,
		}
	} else if !apierrors.IsNotFound(err) {
		// Transient get error
		return taskhandler.Result{
			Description: fmt.Sprintf("Failed to get maintenance %s job", containerName),
			Error:       err,
			Requeue:     true,
		}
	}

	// Build volumes / mounts only if TLS enabled
	vols, vms := buildTLSVolumesAndMounts(etcd)

	// Labels (reuse compaction job network labels for consistency)
	labels := druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)
	labels[druidv1alpha1.LabelAppNameKey] = task.Name
	labels[druidv1alpha1.LabelComponentKey] = common.ComponentNameSnapshotCompactionJob
	labels["networking.gardener.cloud/to-dns"] = "allowed"
	labels["networking.gardener.cloud/to-private-networks"] = "allowed"
	labels["networking.gardener.cloud/to-public-networks"] = "allowed"

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      task.Name,
			Namespace: task.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         druidv1alpha1.SchemeGroupVersion.String(),
					BlockOwnerDeletion: ptr.To(true),
					Controller:         ptr.To(true),
					Kind:               druidv1alpha1.SchemeGroupVersion.WithKind("EtcdOpsTask").Kind,
					Name:               task.Name,
					UID:                task.UID,
				},
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: ptr.To(activeDeadlineSeconds),
			Completions:           ptr.To[int32](1),
			BackoffLimit:          ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:            druidv1alpha1.GetServiceAccountName(etcd.ObjectMeta),
					RestartPolicy:                 corev1.RestartPolicyNever,
					ActiveDeadlineSeconds:         ptr.To(activeDeadlineSeconds),
					TerminationGracePeriodSeconds: ptr.To[int64](60),
					Containers: []corev1.Container{
						{
							Name:            containerName,
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							VolumeMounts:    vms,
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: ptr.To(false),
							},
						},
					},
					Volumes: vols,
				},
			},
		},
	}

	if err := cl.Create(ctx, job); err != nil {
		return taskhandler.Result{
			Description: fmt.Sprintf("Failed to create maintenance %s job", containerName),
			Error:       err,
			Requeue:     true,
		}
	}

	return taskhandler.Result{
		Description: fmt.Sprintf("Maintenance %s job created", containerName),
		Requeue:     true,
	}
}
