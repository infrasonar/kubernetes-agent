import os
from pylibagent.agent import Agent
from lib.check.checkKubernetes import CheckKubernetes
from lib.version import __version__ as version


if __name__ == '__main__':
    # Update ASSET_ID and set a default for the kubernetes agent
    ASSET_ID = os.getenv('ASSET_ID', '/data/.asset.json')
    os.environ['ASSET_ID'] = ASSET_ID

    checks = [CheckKubernetes]
    Agent('kubernetes', version).start(checks, asset_kind='Speed')
