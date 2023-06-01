import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from typing import Dict, List, Any
from pylibagent.check import CheckBase
from .utils import dfmt


class CheckKubernetes(CheckBase):
    key = 'kubernetes'
    interval = int(os.getenv('CHECK_INTERVAL', '300'))

    @classmethod
    async def run(cls):
        if cls.interval == 0:
            raise Exception(f'{cls.key} is disabled')

        if int(os.getenv('IN_CLUSTER', '1')):
            await config.load_incluster_config()
        else:
            await config.load_kube_config()

        async with ApiClient() as api:

            v1 = client.CoreV1Api(api)
            res = await v1.list_namespace()
            namespaces = [
                {
                    'name': i.metadata.name,
                    'phase': i.status.phase,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                }
                for i in res.items
            ]

            res = await v1.list_node(pretty='false')
            nodes = [
                {
                    'name': i.metadata.name,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                    'allocatable_cpu': dfmt(i.status.allocatable['cpu']),
                    'allocatable_memory': dfmt(i.status.allocatable['memory']),
                    # 'allocatable_pods': dfmt(i.status.allocatable['pods']),

                    'capacity_cpu': dfmt(i.status.capacity['cpu']),
                    'capacity_memory': dfmt(i.status.capacity['memory']),
                    # 'capacity_pods': dfmt(i.status.capacity['pods']),

                    'architecture': i.status.node_info.architecture,
                    'container_runtime_version':
                    i.status.node_info.container_runtime_version,
                    'kernel_version': i.status.node_info.kernel_version,
                    'kube_proxy_version':
                    i.status.node_info.kube_proxy_version,
                    'kubelet_version': i.status.node_info.kubelet_version,
                    'operating_system': i.status.node_info.operating_system,

                    'conditions': [
                        c.type
                        for c in i.status.conditions
                        if c.status == 'True'  # lib gives raw values
                    ]
                }
                for i in res.items
            ]

            res = await v1.list_pod_for_all_namespaces()
            pods = [
                {
                    'name': f'{i.metadata.namespace}/{i.metadata.name}',
                    'namespace': i.metadata.namespace,
                    'phase': i.status.phase,
                    'pod_name': i.metadata.name,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                }
                for i in res.items
            ]
            containers = [
                {
                    'name': 
                    f'{i.metadata.namespace}/{i.metadata.name}/{c.name}',
                    'container_name': c.name,
                    'namespace': i.metadata.namespace,
                    'pod': f'{i.metadata.namespace}/{i.metadata.name}',
                    'ports': c.ports and [
                        f'{p.protocol}:{p.container_port}'
                        for p in c.ports
                    ],
                    'limits_cpu':c.resources.limits and
                    dfmt(c.resources.limits.get('cpu')),
                    'limits_memory': c.resources.limits and
                    dfmt(c.resources.limits.get('memory')),
                    'requests_cpu': c.resources.requests and
                    dfmt(c.resources.requests.get('cpu')),
                    'requests_memory': c.resources.requests and
                    dfmt(c.resources.requests.get('memory')),
                }
                for i in res.items
                for c in i.spec.containers

            ]

        return {
            'namespaces': namespaces,
            'nodes': nodes,
            'pods': pods,
            'containers': containers,
        }
