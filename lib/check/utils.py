from typing import Optional, Union

e3_shift = 8
e3_lk = 'yzafpnμm KMGTPEZYXWVU'


def dfmt(val: Optional[str], as_float: bool = False,
         ) -> Union[int, float, None]:
    if val is None:
        return
    if val.isdigit():
        return float(val) if as_float else int(val)
    elif val.endswith('i'):
        try:
            e3 = e3_lk.index(val[-2]) - e3_shift
            val_ = int(val[:-2]) * 1024 ** e3
            return float(val_) if as_float else val_
        except Exception:
            return
    else:
        try:
            e3 = e3_lk.index(val[-1]) - e3_shift
            val_ = int(val[:-1]) * 1000 ** e3
            return float(val_) if as_float else val_
        except Exception:
            return
