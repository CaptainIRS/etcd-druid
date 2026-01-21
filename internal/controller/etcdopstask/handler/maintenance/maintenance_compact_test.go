package maintenance

// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

import (
	"context"
	"fmt"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/etcd-druid/internal/client/kubernetes"
	"github.com/gardener/etcd-druid/internal/common"
	"github.com/gardener/etcd-druid/internal/images"

	testutils "github.com/gardener/etcd-druid/test/utils"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	. "github.com/onsi/gomega"
)

func TestMaintenanceCompactAdmit(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Setup: Ready Etcd without TLS
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue})
	task := buildEtcdOpsTaskWithMaintenanceCompact("compact-task", testutils.TestNamespace, testutils.TestEtcdName, 600)

	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd).Build()
	imgVec, err := images.CreateImageVector()
	g.Expect(err).To(BeNil())

	handler, err := NewCompact(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	// Act
	result := handler.Admit(context.Background())

	// Assert
	g.Expect(result.Error).To(BeNil())
	g.Expect(result.Requeue).To(BeFalse())
	g.Expect(result.Description).To(Equal("Admit check passed"))
}

func TestMaintenanceCompactAdmit_ClientTLSMisconfigured(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Setup: Etcd with client TLS configured but missing ClientTLSSecretRef name
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).
		WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue})
	// Manually set ClientUrlTLS with CA but empty client secret name
	etcd.Spec.Etcd.ClientUrlTLS = &druidv1alpha1.TLSConfig{
		TLSCASecretRef: druidv1alpha1.SecretReference{
			SecretReference: corev1.SecretReference{Name: "client-url-ca-etcd"},
		},
		// Intentionally leave ClientTLSSecretRef.Name empty to simulate misconfiguration
		ClientTLSSecretRef: corev1.SecretReference{Name: ""},
	}

	// Provide the CA secret so the handler reaches the "client TLS not configured" check
	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "client-url-ca-etcd", Namespace: testutils.TestNamespace},
		Data:       map[string][]byte{"ca.crt": []byte("dummy")},
	}

	task := buildEtcdOpsTaskWithMaintenanceCompact("compact-task", testutils.TestNamespace, testutils.TestEtcdName, 600)

	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd, caSecret).Build()
	imgVec, err := images.CreateImageVector()
	g.Expect(err).To(BeNil())

	handler, err := NewCompact(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	// Act
	result := handler.Admit(context.Background())

	// Assert: Terminal error, no requeue
	g.Expect(result.Error).ToNot(BeNil())
	g.Expect(result.Requeue).To(BeFalse())
	g.Expect(result.Description).To(Equal("Client TLS secret not configured"))
}

func TestMaintenanceCompactExecute_CreatesJob_NoTLS(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Setup: Ready Etcd without TLS; runtime components enabled -> expect service-endpoints arg
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue})
	task := buildEtcdOpsTaskWithMaintenanceCompact("compact-task", testutils.TestNamespace, testutils.TestEtcdName, 777)

	// Fake client with Etcd, no Job present
	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd).Build()

	// Image vector with a deterministic etcd-backup-restore image
	imgVec := testutils.CreateImageVector(true, true)

	// Create handler
	handler, err := NewCompact(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	// Act: Execute should create the Job and request a requeue
	result := handler.Execute(context.Background())
	g.Expect(result.Error).To(BeNil())
	g.Expect(result.Requeue).To(BeTrue())
	g.Expect(result.Description).To(Equal("Maintenance maintenance-compact job created"))

	// Assert: Job exists with expected args
	createdJob := &batchv1.Job{}
	g.Expect(cl.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, createdJob)).To(BeNil())

	// Validate basic Job fields
	g.Expect(createdJob.Spec.Template.Spec.ServiceAccountName).To(Equal(druidv1alpha1.GetServiceAccountName(etcd.ObjectMeta)))
	g.Expect(createdJob.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Compact.ActiveDeadlineSeconds)))
	g.Expect(createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Compact.ActiveDeadlineSeconds)))

	// Validate container args include maintenance compact command and endpoints
	g.Expect(createdJob.Spec.Template.Spec.Containers).To(HaveLen(1))
	jobContainer := createdJob.Spec.Template.Spec.Containers[0]
	g.Expect(jobContainer.Image).To(Equal(fmt.Sprintf("%s:%s", testutils.TestImageRepo, testutils.ETCDBRImageTag)))
	g.Expect(jobContainer.Args).To(ContainElement("maintenance"))
	g.Expect(jobContainer.Args).To(ContainElement("compact"))
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--endpoints=http://%s-local:%d", etcd.Name, ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient))))
	// Service endpoints should be set when runtime component creation is enabled (default)
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--service-endpoints=http://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient))))
	// Insecure flags should be set (no TLS)
	g.Expect(jobContainer.Args).To(ContainElement("--insecure-transport=true"))
	g.Expect(jobContainer.Args).To(ContainElement("--insecure-skip-tls-verify=true"))

	// Validate no TLS volumes mounted
	g.Expect(jobContainer.VolumeMounts).To(BeEmpty())
	g.Expect(createdJob.Spec.Template.Spec.Volumes).To(BeEmpty())

}

func TestMaintenanceCompactExecute_CreatesJob_WithTLS(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Setup: Ready Etcd with client TLS configured; provide CA and client secrets
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithClientTLS().WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue})

	// Provide secrets referenced by ClientUrlTLS
	caSecretName := etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.Name
	clientSecretName := etcd.Spec.Etcd.ClientUrlTLS.ClientTLSSecretRef.Name
	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: caSecretName, Namespace: testutils.TestNamespace},
		Data:       map[string][]byte{ptr.Deref(etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.DataKey, "ca.crt"): []byte("dummy")},
	}
	clientSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: clientSecretName, Namespace: testutils.TestNamespace},
		Data:       map[string][]byte{"tls.crt": []byte("dummy"), "tls.key": []byte("dummy")},
	}

	task := buildEtcdOpsTaskWithMaintenanceCompact("compact-task-tls", testutils.TestNamespace, testutils.TestEtcdName, 999)

	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd, caSecret, clientSecret).Build()
	imgVec := testutils.CreateImageVector(true, true)

	handler, err := NewCompact(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	// Act
	result := handler.Execute(context.Background())
	g.Expect(result.Error).To(BeNil())
	g.Expect(result.Requeue).To(BeTrue())
	g.Expect(result.Description).To(Equal("Maintenance maintenance-compact job created"))

	// Assert: Job exists with expected TLS args and volumes
	createdJob := &batchv1.Job{}
	g.Expect(cl.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, createdJob)).To(BeNil())

	g.Expect(createdJob.Spec.Template.Spec.Containers).To(HaveLen(1))
	jobContainer := createdJob.Spec.Template.Spec.Containers[0]

	// TLS flags and https endpoints
	dataKey := ptr.Deref(etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.DataKey, "ca.crt")
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--cacert=%s/%s", common.VolumeMountPathEtcdCA, dataKey)))
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--cert=%s/tls.crt", common.VolumeMountPathEtcdClientTLS)))
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--key=%s/tls.key", common.VolumeMountPathEtcdClientTLS)))
	g.Expect(jobContainer.Args).To(ContainElement("--insecure-transport=false"))
	g.Expect(jobContainer.Args).To(ContainElement("--insecure-skip-tls-verify=false"))
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--endpoints=https://%s-local:%d", etcd.Name, ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient))))
	// Service endpoints should be https when TLS is enabled
	g.Expect(jobContainer.Args).To(ContainElement(fmt.Sprintf("--service-endpoints=https://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient))))

	// TLS volumes mounted
	g.Expect(jobContainer.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      common.VolumeNameEtcdCA,
		MountPath: common.VolumeMountPathEtcdCA,
	}))
	g.Expect(jobContainer.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      common.VolumeNameEtcdClientTLS,
		MountPath: common.VolumeMountPathEtcdClientTLS,
	}))
	// Volumes present
	g.Expect(createdJob.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
		Name: common.VolumeNameEtcdCA,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: caSecretName},
		},
	}))
	g.Expect(createdJob.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
		Name: common.VolumeNameEtcdClientTLS,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: clientSecretName},
		},
	}))
}

// buildEtcdOpsTaskWithMaintenanceCompact constructs an EtcdOpsTask with maintenance.compact config.
func buildEtcdOpsTaskWithMaintenanceCompact(taskName, namespace, etcdName string, activeDeadlineSeconds int32) *druidv1alpha1.EtcdOpsTask {
	return &druidv1alpha1.EtcdOpsTask{
		ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: namespace},
		Spec: druidv1alpha1.EtcdOpsTaskSpec{
			Config: druidv1alpha1.EtcdOpsTaskConfig{
				Maintenance: &druidv1alpha1.MaintenanceConfig{
					Compact: &druidv1alpha1.MaintenanceCompactConfig{
						ActiveDeadlineSeconds: activeDeadlineSeconds,
					},
				},
			},
			TTLSecondsAfterFinished: ptr.To[int32](3600),
			EtcdName:                ptr.To(etcdName),
		},
	}
}

// mustCreateTestImageVector creates a test image vector with given key->image mapping.

// Helper to get an existing Job from fake client (delegates to test utils if available).
