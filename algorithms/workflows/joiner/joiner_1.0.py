import xarray as xr
import glob, os,sys

output=None
xarrs=xarrs.values()
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.combine_first(_xarr)

#output=xr.auto_combine(list(xarrs))
#output=xr.open_mfdataset("/source_storage/results/compuesto_de_medianas/compuesto-temporal-medianas-wf_1.0/*.nc")
#output=xr.merge(list(xarrs))
