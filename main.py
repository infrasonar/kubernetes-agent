from pylibagent.agent import Agent
from lib.check.checkKubernetes import CheckKubernetes
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = [CheckKubernetes]
    Agent('kubernetes', version).start(checks, asset_kind='Kubernetes')
