import xarray as xr
import numpy as np
import glob, os,sys

output=None
xarrs=xarrs.values()
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.combine_first(_xarr)

output.astype(np.int32)
