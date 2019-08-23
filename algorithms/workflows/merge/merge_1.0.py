import xarray as xr
import glob, os,sys

nodata=-9999
output=None
xarrs=xarrs.values()
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.merge(_xarr)
        #output=output.merge(_xarr,fill_value=nodata)

#xarray.merge(objects, compat='no_conflicts', join='outer', fill_value=<NA>)
#output=xr.auto_combine(list(xarrs))
#output=xr.open_mfdataset("/source_storage/results/compuesto_de_medianas/compuesto-temporal-medianas-wf_1.0/*.nc")
#output=xr.merge(list(xarrs))
