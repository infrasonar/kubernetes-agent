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
            res = await v1.list_pod_for_all_namespaces()

        pods = [
            {
                'name': i.metadata.name,
                'namespace': i.metadata.namespace,
                'phase': i.status.phase,
            }
            for i in res.items
        ]

        return {
            'pods': pods,
        }
