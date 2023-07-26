import logging
import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from typing import Dict, List, Any
from pylibagent.check import CheckBase
from .utils import dfmt


LABEL_NODE_ROLE_PREFIX = 'node-role.kubernetes.io/'


def is_none(inp: Any):
    return inp is None or inp == 'None'


def on_node(item) -> dict:
    roles = []
    for label in item.metadata.labels:
        if label.startswith(LABEL_NODE_ROLE_PREFIX):
            role = label[len(LABEL_NODE_ROLE_PREFIX):]
            if role:
                roles.append(role)

    status = []
    for c in item.status.conditions:
        if c.type == 'Ready':
            status.append('Ready' if c.status == 'True' else 'NotReady')
            break
    else:
        status.append('Unknown')
    if item.spec.unschedulable:
        status.append('SchedulingDisabled')

    return {
        'roles': sorted(roles),
        'status': ','.join(status),
    }


def on_node_metrics(item, metrics: dict) -> dict:
    ky = item.metadata.name
    percent_cpu = None
    percent_memory = None
    usage_cpu = None
    usage_memory = None

    try:
        usage_cpu = dfmt(metrics[ky]['usage']['cpu'], True)
        percent_cpu = \
            usage_cpu / dfmt(item.status.allocatable['cpu'], True) * 100.0
    except Exception:
        pass

    try:
        usage_memory = dfmt(metrics[ky]['usage']['memory'])
        percent_memory = \
            usage_memory / dfmt(item.status.allocatable['memory']) * 100.0
    except Exception:
        pass

    return {
        'percent_cpu': percent_cpu,
        'percent_memory': percent_memory,
        'usage_cpu': usage_cpu,
        'usage_memory': usage_memory,
    }


def on_pod(item) -> dict:
    restarts = 0
    total_containers = len(item.spec.containers)
    ready_containers = 0
    reason = item.status.phase
    for c in item.status.conditions:
        if c.type == 'PodScheduled' and c.reason == 'SchedulingGated':
            reason = 'SchedulingGated'

    initializing = False
    if item.status.init_container_statuses is not None:
        for i, cs in enumerate(item.status.init_container_statuses):
            restarts += cs.restart_count
            terminated = cs.state.terminated
            waiting = cs.state.waiting
            if terminated is not None and terminated.exit_code == 0:
                continue
            elif terminated is not None:
                if not terminated.reason:
                    if terminated.signal != 0:
                        reason = f'Init.Signal:{terminated.signal}'
                    else:
                        reason = f'Init.ExitCode:{terminated.exit_code}'
                else:
                    reason = f'Init:{terminated.reason}'
                initializing = True
            elif waiting is not None and len(waiting.reason) and \
                    waiting.reason != 'PodInitializing':
                reason = f'Init:{waiting.reason}'
                initializing = True
            else:
                reason = f'Init:({i}/{len(item.spec.init_containers)})'
                initializing = True
            break

    if not initializing:
        restarts = 0
        has_running = False
        for cs in item.status.container_statuses:
            restarts += cs.restart_count
            terminated = cs.state.terminated
            waiting = cs.state.waiting
            if waiting is not None and waiting.reason:
                reason = waiting.reason
            elif terminated is not None:
                if not terminated.reason:
                    if terminated.signal != 0:
                        reason = f'Signal:{terminated.signal}'
                    else:
                        reason = f'ExitCode:{terminated.exit_code}'
                else:
                    reason = terminated.reason
            elif cs.ready and cs.state.running is not None:
                has_running = True
                ready_containers += 1

        if reason == 'Completed' and has_running:
            if any(c.type == 'Ready' and c.status
                    for c in item.status.conditions):
                reason = 'Running'
            else:
                reason = 'NotReady'

    if item.metadata.deletion_timestamp is not None:
        if item.status.reason == 'NodeLost':
            reason = 'Unknown'
        else:
            reason = 'Terminating'

    return {
        'containers': total_containers,
        'ready_containers': ready_containers,
        'restarts': restarts,
        'status': reason,
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


def svc_external_ips(item) -> dict:
    spec = item.spec
    status = item.status

    if spec.type in ('ClusterIP', 'NodePort'):
        return [] if is_none(spec.external_ips) else spec.external_ips
    elif spec.type == 'LoadBalancer':
        ips = [] if is_none(spec.external_ips) else spec.external_ips
        for i in status.load_balancer.ingress:
            if i.ip != '':
                ips.append(i.ip)
            elif i.hostname != '':
                ips.append(i.hostname)
        return ips
    return []


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
            cust = client.ApiregistrationV1Api(api)
            res = await cust.list_api_service()
            apis = {
                i.metadata.name:
                {
                    'name': i.metadata.name,
                    'available': any(
                        c.type == 'Available' and c.status
                        for c in i.status.conditions
                    ),
                    'service': i.spec.service and (
                        f'{i.spec.service.namespace}/{i.spec.service.name}'
                    ),
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                }
                for i in res.items
            }

            metrics_api = 'v1beta1.metrics.k8s.io'
            if metrics_api in apis and apis[metrics_api]['available']:
                cust = client.CustomObjectsApi(api)
                res = await cust.list_cluster_custom_object(
                    'metrics.k8s.io', 'v1beta1', 'nodes')
                node_metrics = {
                    i['metadata']['name']: i
                    for i in res['items']
                }

                res = await cust.list_cluster_custom_object(
                    'metrics.k8s.io', 'v1beta1', 'pods')
                pod_metrics = {
                    (
                        i['metadata']['namespace'],
                        i['metadata']['name']
                    ): {
                        c['name']: c
                        for c in i['containers']
                    }
                    for i in res['items']
                }
            else:
                logging.warning(
                    f"API Service `{metrics_api}` is not available; "
                    "make sure the metrics server is installed and check if "
                    "the api service is running using `kubectl get "
                    "apiservices`; see: "
                    "https://github.com/kubernetes-sigs/metrics-server")
                node_metrics = {}
                pod_metrics = {}

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
                    **on_node(i),
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
                    **on_pod(i),
                    **on_pod_metrics(i, pod_metrics)
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
                    'restarts': sum(
                        cs.restart_count
                        for cs in i.status.container_statuses
                        if cs.name == c.name
                    ),
                    **on_container_metrics(i, c, pod_metrics)
                }
                for i in res.items
                for c in i.spec.containers

            ]

            res = await v1.list_persistent_volume_claim_for_all_namespaces()
            pvcs = [
                {
                    'name': f'{i.metadata.namespace}/{i.metadata.name}',
                    'namespace': i.metadata.namespace,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                    'storage_class': i.spec.storage_class_name,
                    'volume_name': i.spec.volume_name,
                    'phase': i.status.phase,
                    'access_modes': sorted(i.status.access_modes),
                    'capacity': dfmt(i.status.capacity.get('storage')),
                }
                for i in res.items
            ]

            res = await v1.list_service_for_all_namespaces()
            svcs = [
                {
                    'name': f'{i.metadata.namespace}/{i.metadata.name}',
                    'namespace': i.metadata.namespace,
                    'creation_timestamp':
                    int(i.metadata.creation_timestamp.timestamp()),
                    'type': i.spec.type,
                    'cluster_ip': None if is_none(i.spec.cluster_ip)
                        else i.spec.cluster_ip,
                    'external_ips': sorted(svc_external_ips(i)),
                    'ports': sorted(
                        f'{p.port}/{p.protocol}'
                        for p in i.spec.ports
                    ),
                }
                for i in res.items
            ]

        return {
            'apiservices': tuple(apis.values()),
            'namespaces': namespaces,
            'nodes': nodes,
            'pods': pods,
            'pvcs': pvcs,
            'services': svcs,
            'containers': containers,
        }
