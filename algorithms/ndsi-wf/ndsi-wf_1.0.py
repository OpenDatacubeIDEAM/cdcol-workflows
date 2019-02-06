#!/usr/bin/python3
# coding=utf8
import xarray as xr
import numpy as np
print("Indice de nieve de diferencia normalizada")


period_swir1 = xarr0["swir1"].values
period_green = xarr0["green"].values
mask_nan=np.logical_or(np.isnan(period_swir1), np.isnan(period_green))
period_ndsi = (period_green-period_swir1)/ (period_green+period_swir1)

period_ndsi[mask_nan]=np.nan
period_ndsi[period_ndsi>1]=1.1
period_ndsi[period_ndsi<-1]=-1.1


ncoords=[]
xdims=[]
xcords={}
for x in xarr0.coords:
	if(x!='time'):
		ncoords.append((x, xarr0.coords[x]))
		xdims.append(x)
		xcords[x]=xarr0.coords[x]
variables = {"ndsi": xr.DataArray(period_ndsi, dims=xdims, coords=ncoords)}
output = xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
	output.coords[x].attrs["units"]=xarr0.coords[x].units
