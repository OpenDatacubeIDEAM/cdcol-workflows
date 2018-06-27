import xarray as xr
import numpy as np
print "Excecuting ndvi-wf v1 "


nbar = xarr0
period_red = nbar["red"].values
period_nir = nbar["nir"].values
mask_nan=np.logical_or(np.isnan(period_red), np.isnan(period_nir))
period_nvdi = np.true_divide( np.subtract(period_nir,period_red) , np.add(period_nir,period_red) )
period_nvdi[mask_nan]=np.nan
#Hace un clip para evitar valores extremos. 
period_nvdi[period_nvdi>1]=1.1
period_nvdi[period_nvdi<-1]=-1.1
import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"ndvi": xr.DataArray(period_nvdi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
