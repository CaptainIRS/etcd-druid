# Manage S3 Emulator

This document is a step-by-step guide on how to configure, deploy and cleanup [AWS Emulator](https://github.com/aws/aws-emulator), within a [kind](https://kind.sigs.k8s.io/) cluster. AWS Emulator emulates AWS services locally, which allows the `Etcd` cluster to interact with AWS S3. This setup is ideal for local development and testing.

## 00-Prerequisites

Ensure that you have setup the development environment as per the [documentation](../../development/prepare-dev-environment.md).

> **Note:** It is assumed that you have already created kind cluster and the `KUBECONFIG` is pointing to this Kubernetes cluster.

### Installing AWS CLI

To interact with `AWS Emulator` you must also install the AWS CLI `(version >=1.29.0 or version >=2.13.0)`
On macOS run:

```bash
brew install awscli
```

For other OS, please check the [AWS CLI installation documentation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

## 01-Deploy AWS Emulator

```bash
make deploy-aws-emulator
```

The above make target will deploy `AWS Emulator` in the kind cluster.

## 02-Setup S3 Bucket

Configure `AWS CLI` to interact with AWS Emulator by setting the necessary environment variables. This configuration redirects S3 commands to the AWS Emulator endpoint and provides the required credentials for authentication.

```bash
export AWS_ENDPOINT_URL_S3="https://localhost:9297"
export AWS_ACCESS_KEY_ID=ACCESSKEYAWSUSER
export AWS_SECRET_ACCESS_KEY=sEcreTKey
export AWS_DEFAULT_REGION=us-east-2
```

AWS Emulator uses HTTPS with a self-signed certificate. Pass `--no-verify-ssl` to AWS CLI commands for this endpoint.

Create a S3 bucket using the following command:

```bash
aws --no-verify-ssl s3api create-bucket --bucket <bucket-name> --region <region> --create-bucket-configuration LocationConstraint=<region> --acl private
```

To verify if the bucket has been created, you can use the following command:

```bash
aws --no-verify-ssl s3api head-bucket --bucket <bucket-name>
```

## 03-Configure Secret

Connection details for an Azure S3 Object Store are put into a Kubernetes [Secret](https://kubernetes.io/docs/concepts/configuration/secret/). Apply the Kubernetes Secret manifest through:

```bash
kubectl apply -f examples/objstore-emulator/etcd-secret-aws-emulator.yaml
```

> **Note:** The secret created should be referred to in the `Etcd` CR in `spec.backup.store.secretRef`.

## 04-Cleanup

To clean the setup,, unset the environment variable set in step-03 above and delete the AWS Emulator deployment:

```bash
unset AWS_ENDPOINT_URL_S3 AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
kubectl delete -f ./hack/e2e-test/infrastructure/aws-emulator/aws-emulator.yaml
```
