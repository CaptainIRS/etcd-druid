// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
)

const (
	// ErrInsufficientPermissions represents the error in case of insufficient permissions (RBAC) to perform an operation.
	ErrInsufficientPermissions druidapicommon.ErrorCode = "ERR_INSUFFICIENT_PERMISSIONS"
	// ErrGetEtcd represents the error in case of fetching etcd object.
	ErrGetEtcd druidapicommon.ErrorCode = "ERR_GET_ETCD"
	// ErrDuplicateTask represents the error in case of a duplicate task for the same etcd.
	ErrDuplicateTask druidapicommon.ErrorCode = "ERR_DUPLICATE_TASK"
	// ErrGetCASecret represents the error in case of failure in fetching the CA secret
	ErrGetCASecret druidapicommon.ErrorCode = "ERR_GET_CA_SECRET" // #nosec G101
	// ErrCADataKeyNotFound represents the error when CA cert data key is not found in secret
	ErrCADataKeyNotFound druidapicommon.ErrorCode = "ERR_CA_DATA_KEY_NOT_FOUND"
	// ErrAppendCACerts represents the error when failed to append CA certs from secret
	ErrAppendCACerts druidapicommon.ErrorCode = "ERR_APPEND_CA_CERTS"
	// ErrDeleteEtcdOpsTask represents the error in case of failure in deleting EtcdOpsTask object.
	ErrDeleteEtcdOpsTask druidapicommon.ErrorCode = "ERR_DELETE_ETCD_OPS_TASK"
)

// Result defines the result of a task execution.
type Result struct {
	Description string
	Error       error
	Requeue     bool
}

// Handler defines the interface for task execution.
type Handler interface {
	// Admit checks if the task is permitted to run. This is a one-time gate; once passed, it is not checked again for the same task execution.
	Admit(ctx context.Context) Result
	// Execute executes the main logic of the task. Is executed after Admit has returned a successful result.
	Execute(ctx context.Context) Result
	// Cleanup is invoked by the EtcdOpsTask controller during the task's deletion flow,
	// after the task has reached a terminal state (Succeeded/Failed/Rejected) and its TTL
	// has expired. Implementations should:
	//  - be idempotent and fast;
	//  - prefer managing child resources via ownerReferences so Kubernetes GC can remove
	//    them automatically without explicit deletions;
	//  - return a no-op Result when there is nothing to clean up.
	//
	// Notes:
	//  - The controller skips calling Cleanup for rejected tasks.
	//  - Any Error set in the returned Result will be recorded in task status. Use
	//    Requeue=true only for transient failures that are expected to succeed on retry.
	Cleanup(ctx context.Context) Result
}
