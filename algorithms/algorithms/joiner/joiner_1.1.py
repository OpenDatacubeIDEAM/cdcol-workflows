import xarray as xr
import numpy as np

output=None
xarrs=xarrs.values()
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.combine_first(_xarr)

