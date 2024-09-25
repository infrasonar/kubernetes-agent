from pylibagent.agent import Agent
from lib.check.checkKubernetes import CheckKubernetes
from lib.version import __version__ as version


async def test():

    check_data = \
        await asyncio.wait_for(CheckKubernetes.run(), timeout=90)
    import json

    print(json.dumps(check_data["pods"], indent=4))



if __name__ == '__main__':
    checks = [CheckKubernetes]

    import asyncio, os
    asyncio.run(test())


    # Agent('kubernetes', version).start(checks, asset_kind='Kubernetes')
