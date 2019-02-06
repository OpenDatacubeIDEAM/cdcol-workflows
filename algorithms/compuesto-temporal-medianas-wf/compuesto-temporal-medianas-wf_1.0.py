#!/usr/bin/python3
# coding=utf8
import xarray as xr
import numpy as np
print ("Compuesto temporal de medianas para " + product)
medians = {}
for band in bands:
    datos = xarr0.data_vars[band]
    allNan = ~np.isnan(datos)
    medians[band] = np.nanmedian(datos, 0)
    medians[band][np.sum(allNan, 0) < minValid] = np.nan
del datos


# > **Asignación de coordenadas**
ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords) for k, v in medians.items()}
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units
