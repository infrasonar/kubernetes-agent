import os
import asyncio
import logging
from typing import Dict, List, Any
from pylibagent.check import CheckBase


class CheckKubernetes(CheckBase):
    key = 'kubernetes'
    interval = int(os.getenv('CHECK_XXX_INTERVAL', '900'))
