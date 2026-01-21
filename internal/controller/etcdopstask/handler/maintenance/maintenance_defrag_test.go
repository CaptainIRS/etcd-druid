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
	testutils "github.com/gardener/etcd-druid/test/utils"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	. "github.com/onsi/gomega"
)

func TestMaintenanceDefragAdmit(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Case 1: Ready etcd without TLS should pass admit.
	etcdReadyNoTLS := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithReadyStatus().Build()
	// Explicitly set ConditionTypeReady for IsReady().
	etcdReadyNoTLS.Status.Conditions = append(etcdReadyNoTLS.Status.Conditions, druidv1alpha1.Condition{
		Type:   druidv1alpha1.ConditionTypeReady,
		Status: druidv1alpha1.ConditionTrue,
	})

	task := buildEtcdOpsTaskWithMaintenanceDefrag("defrag-task", testutils.TestNamespace, testutils.TestEtcdName, 480, 600)
	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcdReadyNoTLS).Build()
	imgVec := testutils.CreateImageVector(true, true)

	h, err := NewDefrag(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	admit := h.Admit(context.Background())
	g.Expect(admit.Error).To(BeNil())
	g.Expect(admit.Requeue).To(BeFalse())
	g.Expect(admit.Description).To(Equal("Admit check passed"))

	// Case 2: Client TLS misconfigured (CA present but ClientTLSSecretRef empty) should fail admit (terminal).
	etcdTLSMisconfigured := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithReadyStatus().Build()
	etcdTLSMisconfigured.Status.Conditions = append(etcdTLSMisconfigured.Status.Conditions, druidv1alpha1.Condition{
		Type:   druidv1alpha1.ConditionTypeReady,
		Status: druidv1alpha1.ConditionTrue,
	})
	etcdTLSMisconfigured.Spec.Etcd.ClientUrlTLS = &druidv1alpha1.TLSConfig{
		TLSCASecretRef: druidv1alpha1.SecretReference{
			SecretReference: corev1.SecretReference{
				Name: testutils.ClientTLSCASecretName,
			},
			DataKey: ptr.To("ca.crt"),
		},
		ClientTLSSecretRef: corev1.SecretReference{
			Name: "",
		},
	}

	// Provide CA secret so it passes CA secret existence check.
	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testutils.ClientTLSCASecretName, Namespace: testutils.TestNamespace},
		Data:       map[string][]byte{"ca.crt": []byte("dummy")},
	}

	cl2 := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcdTLSMisconfigured, caSecret).Build()
	h2, err := NewDefrag(cl2, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	admit2 := h2.Admit(context.Background())
	g.Expect(admit2.Error).ToNot(BeNil())
	g.Expect(admit2.Requeue).To(BeFalse())
	g.Expect(admit2.Description).To(Equal("Client TLS secret not configured"))
}

func TestMaintenanceDefragExecute_CreatesJob_NoTLS_WithTimeoutOverride(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Ready etcd without TLS
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{
		Type:   druidv1alpha1.ConditionTypeReady,
		Status: druidv1alpha1.ConditionTrue,
	})

	// Use timeout override from task (777s) and active deadline (999s)
	task := buildEtcdOpsTaskWithMaintenanceDefrag("defrag-task-no-tls", testutils.TestNamespace, testutils.TestEtcdName, 777, 999)
	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd).Build()
	imgVec := testutils.CreateImageVector(true, true)

	h, err := NewDefrag(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	// Execute -> create Job
	run := h.Execute(context.Background())
	g.Expect(run.Error).To(BeNil())
	g.Expect(run.Requeue).To(BeTrue())
	g.Expect(run.Description).To(Equal("Maintenance maintenance-defrag job created"))

	createdJob := &batchv1.Job{}
	g.Expect(cl.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, createdJob)).To(BeNil())

	// Validate deadlines
	g.Expect(createdJob.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Defrag.ActiveDeadlineSeconds)))
	g.Expect(createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Defrag.ActiveDeadlineSeconds)))

	// Validate container args include maintenance defrag and timeout override
	g.Expect(createdJob.Spec.Template.Spec.Containers).To(HaveLen(1))
	c := createdJob.Spec.Template.Spec.Containers[0]
	g.Expect(c.Image).To(Equal(fmt.Sprintf("%s:%s", testutils.TestImageRepo, testutils.ETCDBRImageTag)))
	g.Expect(c.Args).To(ContainElement("maintenance"))
	g.Expect(c.Args).To(ContainElement("defrag"))
	g.Expect(c.Args).To(ContainElement("--insecure-transport=true"))
	g.Expect(c.Args).To(ContainElement("--insecure-skip-tls-verify=true"))
	clientPort := ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient)
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--endpoints=http://%s-local:%d", etcd.Name, clientPort)))
	// service-endpoints is included when runtime component creation is enabled (default)
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--service-endpoints=http://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), clientPort)))
	// timeout override flag present
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--etcd-defrag-timeout=%ds", task.Spec.Config.Maintenance.Defrag.TimeoutSeconds)))

	// No TLS volumes/mounts expected
	g.Expect(c.VolumeMounts).To(BeEmpty())
	g.Expect(createdJob.Spec.Template.Spec.Volumes).To(BeEmpty())
}

func TestMaintenanceDefragExecute_CreatesJob_WithTLS_WithTimeoutOverride(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	// Ready etcd with client TLS
	etcd := testutils.EtcdBuilderWithDefaults(testutils.TestEtcdName, testutils.TestNamespace).WithClientTLS().WithReadyStatus().Build()
	etcd.Status.Conditions = append(etcd.Status.Conditions, druidv1alpha1.Condition{
		Type:   druidv1alpha1.ConditionTypeReady,
		Status: druidv1alpha1.ConditionTrue,
	})

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

	task := buildEtcdOpsTaskWithMaintenanceDefrag("defrag-task-tls", testutils.TestNamespace, testutils.TestEtcdName, 321, 888)
	cl := testutils.NewTestClientBuilder().WithScheme(kubernetes.Scheme).WithObjects(etcd, caSecret, clientSecret).Build()
	imgVec := testutils.CreateImageVector(true, true)

	h, err := NewDefrag(cl, task, nil, imgVec)
	g.Expect(err).To(BeNil())

	run := h.Execute(context.Background())
	g.Expect(run.Error).To(BeNil())
	g.Expect(run.Requeue).To(BeTrue())
	g.Expect(run.Description).To(Equal("Maintenance maintenance-defrag job created"))

	createdJob := &batchv1.Job{}
	g.Expect(cl.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, createdJob)).To(BeNil())

	// Validate deadlines
	g.Expect(createdJob.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Defrag.ActiveDeadlineSeconds)))
	g.Expect(createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).ToNot(BeNil())
	g.Expect(*createdJob.Spec.Template.Spec.ActiveDeadlineSeconds).To(Equal(int64(task.Spec.Config.Maintenance.Defrag.ActiveDeadlineSeconds)))

	// Validate TLS args and timeout override
	g.Expect(createdJob.Spec.Template.Spec.Containers).To(HaveLen(1))
	c := createdJob.Spec.Template.Spec.Containers[0]
	g.Expect(c.Image).To(Equal(fmt.Sprintf("%s:%s", testutils.TestImageRepo, testutils.ETCDBRImageTag)))
	g.Expect(c.Args).To(ContainElement("maintenance"))
	g.Expect(c.Args).To(ContainElement("defrag"))
	dataKey := ptr.Deref(etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.DataKey, "ca.crt")
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--cacert=%s/%s", common.VolumeMountPathEtcdCA, dataKey)))
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--cert=%s/tls.crt", common.VolumeMountPathEtcdClientTLS)))
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--key=%s/tls.key", common.VolumeMountPathEtcdClientTLS)))
	g.Expect(c.Args).To(ContainElement("--insecure-transport=false"))
	g.Expect(c.Args).To(ContainElement("--insecure-skip-tls-verify=false"))
	clientPort := ptr.Deref(etcd.Spec.Etcd.ClientPort, common.DefaultPortEtcdClient)
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--endpoints=https://%s-local:%d", etcd.Name, clientPort)))
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--service-endpoints=https://%s:%d", druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), clientPort)))
	g.Expect(c.Args).To(ContainElement(fmt.Sprintf("--etcd-defrag-timeout=%ds", task.Spec.Config.Maintenance.Defrag.TimeoutSeconds)))

	// Validate volumes/mounts for TLS
	g.Expect(c.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      common.VolumeNameEtcdCA,
		MountPath: common.VolumeMountPathEtcdCA,
	}))
	g.Expect(c.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      common.VolumeNameEtcdClientTLS,
		MountPath: common.VolumeMountPathEtcdClientTLS,
	}))
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

func buildEtcdOpsTaskWithMaintenanceDefrag(taskName, namespace, etcdName string, timeoutSeconds, activeDeadlineSeconds int32) *druidv1alpha1.EtcdOpsTask {
	return &druidv1alpha1.EtcdOpsTask{
		ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: namespace},
		Spec: druidv1alpha1.EtcdOpsTaskSpec{
			Config: druidv1alpha1.EtcdOpsTaskConfig{
				Maintenance: &druidv1alpha1.MaintenanceConfig{
					Defrag: &druidv1alpha1.MaintenanceDefragConfig{
						TimeoutSeconds:        timeoutSeconds,
						ActiveDeadlineSeconds: activeDeadlineSeconds,
					},
				},
			},
			TTLSecondsAfterFinished: ptr.To[int32](3600),
			EtcdName:                ptr.To(etcdName),
		},
	}
}
