from typing import Optional

e3_shift = 8
e3_lk = 'yzafpnμm KMGTPEZYXWVU'


def dfmt(val: Optional[str]) -> Optional[int]:
    if val is None:
        return
    try:
        return int(val)
    except Exception:
        return


def dfmt_1000(val: Optional[str]) -> Optional[int]:
    if val is None:
        return
    try:
        e3 = e3_lk.index(val[-1]) - e3_shift
        return int(val[:-1]) * 1000 ** e3
    except Exception:
        return


def dfmt_1024(val: Optional[str]) -> Optional[int]:
    if val is None:
        return
    try:
        e3 = e3_lk.index(val[-2]) - e3_shift
        return int(val[:-2]) * 1024 ** e3
    except Exception:
        return
