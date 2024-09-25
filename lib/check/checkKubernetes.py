import json
import logging
import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from typing import Any, Optional, Union, Dict
from pylibagent.check import CheckBase
from .utils import dfmt
from ..version import __version__ as version


LABEL_NODE_ROLE_PREFIX = 'node-role.kubernetes.io/'


def is_none(inp: Any):
    return inp is None or inp == 'None' or inp == ''


def is_none_empty_str(inp):
    return isinstance(inp, str) and inp != ''


def on_node(item) -> dict:
    roles = []
    for label in item.metadata.labels:
        if label.startswith(LABEL_NODE_ROLE_PREFIX):
            role = label[len(LABEL_NODE_ROLE_PREFIX):]
            if role:
                roles.append(role)

    status = []
    # It is possible that `item.status.conditions` is None; When this is the
    # case we return with status `Unknown` although I'm not sure that this is
    # correct.
    for c in item.status.conditions or []:
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
        allocatable_cpu = dfmt(item.status.allocatable['cpu'], True)
        assert isinstance(usage_cpu, float)
        assert isinstance(allocatable_cpu, float)
        percent_cpu = usage_cpu / allocatable_cpu * 100
    except Exception:
        pass

    try:
        usage_memory = dfmt(metrics[ky]['usage']['memory'])
        allocatable_memory = dfmt(item.status.allocatable['memory'])
        assert isinstance(usage_memory, int)
        assert isinstance(allocatable_memory, int)
        percent_memory = usage_memory / allocatable_memory * 100
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
    last_state: Dict[str, Optional[Union[str, int]]] = {
        'last_state': None,
        'last_state_reason': None,
        'last_state_exit_code': None,
        'last_state_started_at': None,
        'last_state_finished_at': None,
    }
    # It seems that `item.status.conditions` can be None as well, most likely
    # when a pod has status `Failed` with reason `Evicted`. I'm not sure if the
    # reason is correct as when this case happened, the code not have the
    # `or []` part so we only looked at an iteration exception.
    for c in item.status.conditions or []:
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
        if item.status.container_statuses is not None:
            for cs in item.status.container_statuses:
                ls = cs.last_state.terminated
                if ls is not None:
                    last_state['last_state'] = 'Terminated'
                    last_state['last_state_reason'] = ls.reason
                    last_state['last_state_exit_code'] = ls.exit_code
                    last_state['last_state_started_at'] = \
                        ls.started_at.timestamp()
                    last_state['last_state_finished_at'] = \
                        ls.finished_at.timestamp()

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
        **last_state,
    }


def on_pod_metrics(item, metrics: dict) -> dict:
    ky = item.metadata.namespace, item.metadata.name
    usage_cpu = None
    usage_memory = None

    try:
        usage_cpu = sum(v for v in (
            dfmt(c['usage']['cpu'], True)
            for c in metrics[ky].values()
        ) if isinstance(v, float))
    except Exception:
        pass

    try:
        usage_memory = sum(v for v in (
            dfmt(c['usage']['memory'])
            for c in metrics[ky].values()
        ) if isinstance(v, int))
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


def on_pvc_usage_metrics(item, metrics: dict) -> dict:
    try:
        volume = metrics[item.metadata.name]
    except Exception:
        return {}
    try:
        percent = volume['usedBytes'] / volume['capacityBytes'] * 100.0
    except Exception:
        percent = None

    return {
        'available_bytes': volume['availableBytes'],
        'capacity_bytes': volume['capacityBytes'],
        'used_bytes': volume['usedBytes'],
        'percent_used': percent
    }


def ensure_list_none_empty_strings(inp):
    if is_none(inp):
        return []
    if isinstance(inp, str):
        return inp.split(',')
    if isinstance(inp, (list, tuple)):
        return [itm for itm in inp if is_none_empty_str(itm)]
    logging.warning(f'Expecting a list of strings but got: {inp}')
    return []


def svc_external_ips(item) -> list:
    spec = item.spec
    status = item.status

    if spec.type in ('ClusterIP', 'NodePort'):
        return ensure_list_none_empty_strings(spec.external_ips)
    elif spec.type == 'LoadBalancer':
        ips = ensure_list_none_empty_strings(spec.external_ips)
        if not is_none(status.load_balancer.ingress):
            for i in status.load_balancer.ingress:
                if is_none_empty_str(i.ip):
                    ips.append(i.ip)
                elif is_none_empty_str(i.hostname):
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

        try:
            res = await cls._run()
        except Exception:
            logging.exception('Kubernetes exception')
            raise
        else:
            return res

    @classmethod
    async def _run(cls):
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
                    'restarts': 0 if i.status.container_statuses is None else
                        sum(
                            cs.restart_count
                            for cs in i.status.container_statuses
                            if cs.name == c.name
                        ),
                    **on_container_metrics(i, c, pod_metrics)
                }
                for i in res.items
                for c in i.spec.containers

            ]

            pvc_usage = {}
            for node in nodes:
                try:
                    # returns single_quote json;
                    # Requires:
                    #   verb "get" and resource "nodes/proxy" access in
                    #   apiGroup group "".
                    text: str = await v1.connect_get_node_proxy_with_path(
                        node['name'], 'stats/summary')  # type: ignore
                    replaced = text.replace("'", '"')
                    node_summary = json.loads(replaced)
                except Exception as e:
                    msg = str(e) or type(e).__name__
                    logging.warning(f'failed to retrieve pvc usage: {msg}')
                else:
                    for pod in node_summary['pods']:
                        for vol in pod.get('volume', []):
                            pvc_ref = vol.get('pvcRef')
                            if pvc_ref:
                                pvc_usage[pvc_ref['name']] = vol

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
                    **on_pvc_usage_metrics(i, pvc_usage)
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
                    'ports': [] if is_none(i.spec.ports) else sorted(
                        f'{p.port}/{p.protocol}'
                        for p in i.spec.ports
                        if p is not None
                    ),
                }
                for i in res.items
            ]

        return {
            'infrasonar': [{
                'name': 'agent',
                'version': version,
            }],
            'apiservices': tuple(apis.values()),
            'namespaces': namespaces,
            'nodes': nodes,
            'pods': pods,
            'pvcs': pvcs,
            'services': svcs,
            'containers': containers,
        }
