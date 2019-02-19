#Joiner: Une dos o más datasets en uno solo. 
#Los dos datasets deben tener las mismas bandas (variables) y deben tener 
#parámetros: xarrs (generado por el operador reducer), bands (opcional, pero debe existir si en algún dataset hay bandas adicionales)
import xarray as xr
output=None
xarrs=list(xarrs.values())
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.combine_first(_xarr)
# try:
#     bands
# except NameError:
#     bands=xarrs[0].keys()
# for _xarr in xarrs:
#     _undesired=list(set(_xarr.keys())-set(bands+['latitude','longitude','time']))
#     _xarr=_xarr.drop(_undesired)
#     if output is None:
#         output = _xarr
#     else:
#         output=xr.concat([output,_xarr.copy(deep=True)],'time')
#
# output['crs'] = xarrs[0].crs
# output=output.transpose(*xarrs[0].coords.keys())
#output=xr.merge(xarrs, compat='no_conflicts',join='inner')
print(output.coords['time'])