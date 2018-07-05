#Joiner: Une dos o más datasets en uno solo. 
#Los dos datasets deben tener las mismas bandas (variables) y deben tener 
#parámetros: xarrs (generado por el operador reducer), bands (opcional, pero debe existir si en algún dataset hay bandas adicionales)
output=None
try: 
    bands
except NameError:
    bands=xarrs[0].keys()
for _xarr in xarrs: 
    _undesired=list(set(_xarr.keys())-set(bands+['latitude','longitude','time']))
    _xarr=_xarr.drop(_undesired)
    if output is None: 
        output = _xarr
    else: 
        output=xr.concat(output,_xarr.copy(deep=True),'time')

print(output.coords['time'])