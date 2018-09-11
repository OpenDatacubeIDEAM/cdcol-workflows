import xarray as xr 
import numpy as np
print "executing ndsi v1.0-test worflows"

nbar=xarr0
period_swir1 = nbar["swir1"].values
period_green = nbar["green"].values
mask_nan=np.logical_or(np.isnan(period_swir1), np.isnan(period_green))
period_ndsi = np.true_divide(np.subtract(period_swir1, period_green), np.add(period_swir1, period_green))
period_ndsi[mask_nan]=np.nan
period_ndsi[period_ndsi>1]=1.1
period_ndsi[period_ndsi<-1]=-1.1
import xarray as xr 
ncoords=[]
xdims=[]
xcords={}
for x in nbar.coords:
	if(x!='time'):
		ncoords.append((x, nbar.coords[x]))
		xdims.append(x)
		xcords[x]=nbar.coords[x]
variables = {"ndsi": xr.DataArray(period_ndsi, dims=xdims, coords=ncoords)}
output = xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
	output.coords[x].attrs["units"]=nbar.coords[x].units
