
from typing import Dict
import numpy as np
STAT_KEYS = ['n','v_min','p05','p25','p50','v_mean','p75','p95','v_max']

def summarize(values: np.ndarray) -> Dict[str,float]:
    values = values[~np.isnan(values)]
    if values.size == 0:
        out = {k: None for k in STAT_KEYS}
        out["n"] = 0
        return out
    q = np.quantile(values, [0.05,0.25,0.5,0.75,0.95])
    return {
        'n': int(values.size),
        'v_min': float(values.min()),
        'p05': float(q[0]),
        'p25': float(q[1]),
        'p50': float(q[2]),
        'v_mean': float(values.mean()),
        'p75': float(q[3]),
        'p95': float(q[4]),
        'v_max': float(values.max()),
    }
