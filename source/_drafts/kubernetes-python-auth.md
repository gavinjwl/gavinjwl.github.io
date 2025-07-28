---
title:
  '[object Object]': null
tags:
---

## Background

[boto3 do not offer EKS get-token or update-kubeconfig method](https://github.com/boto/boto3/issues/2309)

## References

- [Official Python client library for kubernetes](https://github.com/kubernetes-client/python)
- [Using the Kubernetes Python Client with AWS](https://www.analogous.dev/blog/using-the-kubernetes-python-client-with-aws/)
- [AWS CLI TokenGenerator](https://github.com/aws/aws-cli/blob/274ee71cb3180e557a54f9445cca2b6a7a998d24/awscli/customizations/eks/get_token.py#L90)
- [API Authorization from Outside a EKS Cluster](https://github.com/kubernetes-sigs/aws-iam-authenticator/blob/master/README.md#api-authorization-from-outside-a-cluster)
- [Python Package to get EKS auth token (Alternative to "aws eks get-token ...." CLI)](https://github.com/peak-ai/eks-token)
- [AWS Official Docs: Creating or updating a kubeconfig file for an Amazon EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html)

```python
# %%
import base64
import re
import tempfile

import boto3
import kubernetes
from botocore.signers import RequestSigner


def _get_bearer_token(cluster_id, region):
    '''
    https://github.com/kubernetes-sigs/aws-iam-authenticator/blob/master/README.md#api-authorization-from-outside-a-cluster
    '''
    STS_TOKEN_EXPIRES_IN = 60
    session = boto3.session.Session()

    client = session.client('sts', region_name=region)
    service_id = client.meta.service_model.service_id

    signer = RequestSigner(
        service_id,
        region,
        'sts',
        'v4',
        session.get_credentials(),
        session.events
    )

    params = {
        'method': 'GET',
        'url': 'https://sts.{}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15'.format(region),
        'body': {},
        'headers': {
            'x-k8s-aws-id': cluster_id
        },
        'context': {}
    }

    signed_url = signer.generate_presigned_url(
        params,
        region_name=region,
        expires_in=STS_TOKEN_EXPIRES_IN,
        operation_name=''
    )

    base64_url = base64.urlsafe_b64encode(
        signed_url.encode('utf-8')).decode('utf-8')

    # remove any base64 encoding padding:
    return 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)


def _get_eks_info(cluster_name, region):
    client = boto3.client('eks', region_name=region)
    response = client.describe_cluster(
        name=cluster_name
    )
    eks_cluster = response['cluster']
    endpoint = eks_cluster['endpoint']
    cert = eks_cluster['certificateAuthority']['data']

    # ref: https://www.analogous.dev/blog/using-the-kubernetes-python-client-with-aws/#now-tls
    # protect yourself from automatic deletion
    # cafile = tempfile.NamedTemporaryFile(delete=True)
    cafile = tempfile.NamedTemporaryFile(delete=False)
    cadata = base64.b64decode(cert)
    cafile.write(cadata)
    cafile.flush()

    return (endpoint, cafile)

# %%
def get_kube_client(cluster_name, region):
    token = _get_bearer_token(cluster_name, region)
    (endpoint, cafile) = _get_eks_info(cluster_name, region)

    configuration = kubernetes.config.kube_config.Configuration()
    configuration.verify_ssl = True
    configuration.host = endpoint
    configuration.api_key['authorization'] = token
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration.ssl_ca_cert = cafile.name

    v1 = kubernetes.client.CoreV1Api(
        kubernetes.client.ApiClient(configuration=configuration)
    )
    return v1


v1 = get_kube_client()
ret = v1.list_namespace()
print(ret)
```
