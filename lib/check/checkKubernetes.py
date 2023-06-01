import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from typing import Dict, List, Any
from pylibagent.check import CheckBase
from .utils import dfmt


def on_node_metrics(item, metrics: dict) -> dict:
    ky = item.metadata.name
    percent_cpu = None
    percent_memory = None
    usage_cpu = None
    usage_memory = None

    try:
        usage_cpu = dfmt(metrics[ky]['usage']['cpu'], True)
        percent_cpu = usage_cpu / dfmt(item.status.allocatable['cpu'], True)
    except Exception:
        pass

    try:
        usage_memory = dfmt(metrics[ky]['usage']['memory'])
        percent_memory = usage_memory / dfmt(item.status.allocatable['memory'])
    except Exception:
        pass

    return {
        'percent_cpu': percent_cpu,
        'percent_memory': percent_memory,
        'usage_cpu': usage_cpu,
        'usage_memory': usage_memory,
    }


def on_pod_metrics(item, metrics: dict) -> dict:
    ky = item.metadata.namespace, item.metadata.name
    usage_cpu = None
    usage_memory = None

    try:
        usage_cpu = sum(
            dfmt(c['usage']['cpu'], True)
            for c in metrics[ky].values())
    except Exception:
        pass

    try:
        usage_memory = sum(
            dfmt(c['usage']['memory'])
            for c in metrics[ky].values())
    except Exception:
        pass

    return {
        'usage_cpu': usage_cpu,
        'usage_memory': usage_memory,
    }


def on_container_metrics(item, container, metrics: dict) -> dict:
    ky = item.metadata.namespace, item.metadata.name
    usage_cpu = None
    usage_memory = None

    try:
        usage_cpu = dfmt(metrics[ky][container.name]['usage']['cpu'], True)
    except Exception:
        pass

    try:
        usage_memory = dfmt(metrics[ky][container.name]['usage']['memory'])
    except Exception:
        pass

    return {
        'usage_cpu': usage_cpu,
        'usage_memory': usage_memory,
    }


class CheckKubernetes(CheckBase):
    key = 'kubernetes'
    interval = int(os.getenv('CHECK_INTERVAL', '300'))

    @classmethod
    async def run(cls):
        if cls.interval == 0:
            raise Exception(f'{cls.key} is disabled')

        if int(os.getenv('IN_CLUSTER', '1')):
            config.load_incluster_config()
        else:
            await config.load_kube_config()

        async with ApiClient() as api:

            cust = client.CustomObjectsApi(api)
            res = await cust.list_cluster_custom_object(
                'metrics.k8s.io', 'v1beta1', 'nodes')
            node_metrics = {
                i['metadata']['name']: i
                for i in res['items']
            }

            res = await cust.list_cluster_custom_object(
                'metrics.k8s.io', 'v1beta1', 'pods')
            metrics = {
                (
                    i['metadata']['namespace'],
                    i['metadata']['name']
                ): {
                    c['name']: c
                    for c in i['containers']
                }
                for i in res['items']
            }

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
                    'allocatable_cpu': dfmt(i.status.allocatable['cpu'], True),
                    'allocatable_memory': dfmt(i.status.allocatable['memory']),
                    'allocatable_pods': dfmt(i.status.allocatable['pods']),

                    'capacity_cpu': dfmt(i.status.capacity['cpu'], True),
                    'capacity_memory': dfmt(i.status.capacity['memory']),
                    'capacity_pods': dfmt(i.status.capacity['pods']),

                    'architecture': i.status.node_info.architecture,
                    'container_runtime_version':
                    i.status.node_info.container_runtime_version,
                    'kernel_version': i.status.node_info.kernel_version,
                    'kube_proxy_version':
                    i.status.node_info.kube_proxy_version,
                    'kubelet_version': i.status.node_info.kubelet_version,
                    'operating_system': i.status.node_info.operating_system,
                    **on_node_metrics(i, node_metrics)
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
                    **on_pod_metrics(i, metrics)
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
                    'limits_cpu': c.resources.limits and
                    dfmt(c.resources.limits.get('cpu'), True),
                    'limits_memory': c.resources.limits and
                    dfmt(c.resources.limits.get('memory')),
                    'requests_cpu': c.resources.requests and
                    dfmt(c.resources.requests.get('cpu'), True),
                    'requests_memory': c.resources.requests and
                    dfmt(c.resources.requests.get('memory')),
                    **on_container_metrics(i, c, metrics)
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
