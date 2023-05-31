import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from typing import Dict, List, Any
from pylibagent.check import CheckBase


class CheckKubernetes(CheckBase):
    key = 'kubernetes'
    interval = int(os.getenv('CHECK_XXX_INTERVAL', '900'))

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

            res = await v1.list_node()
            nodes = [
                {
                    'name': i.metadata.name,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                    'allocatable_cpu': i.status.allocatable['cpu'],
                    'allocatable_memory': i.status.allocatable['memory'],
                    # 'allocatable_pods': i.status.allocatable['pods'],

                    'capacity_cpu': i.status.capacity['cpu'],
                    'capacity_memory': i.status.capacity['memory'],
                    # 'capacity_pods': i.status.capacity['pods'],

                    'architecture': i.node_info.architecture,
                    'container_runtime_version': i.node_info.container_runtime_version,
                    'kernel_version': i.node_info.kernel_version,
                    'kube_proxy_version': i.node_info.kube_proxy_version,
                    'kubelet_version': i.node_info.kubelet_version,
                    'operating_system': i.node_info.operating_system,

                    'conditions': [
                        c.type
                        for c in i.status.conditions if c.status]
                }
                for i in res.items
            ]

            res = await v1.list_pod_for_all_namespaces()
            pods = [
                {
                    'name': i.metadata.name,
                    'namespace': i.metadata.namespace,
                    'phase': i.status.phase,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                }
                for i in res.items
            ]
            containers = [
                {
                    'name': c.name,
                    'pod': i.metadata.name,
                    'ports': c.ports and [
                        f'{p.protocol}:{p.container_port}'
                        for p in c.ports
                    ],
                    'resources_limits_cpu':
                    c.resources.limits and c.resources.limits.cpu,
                    'resources_limits_memory':
                    c.resources.limits and c.resources.limits.memory,
                    'resources_requests_cpu':
                    c.resources.requests and c.resources.requests.cpu,
                    'resources_requests_memory':
                    c.resources.requests and c.resources.requests.memory,
                    # TODO volume_mounts?
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
