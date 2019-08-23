import xarray as xr
import numpy as np
print("Proporcion del Indice de Vegetacion")

period_nir = xarr0["nir"].values
period_red = xarr0["red"].values
mask_nan=np.logical_or(np.isnan(period_nir), np.isnan(period_red))
period_rvi = (period_nir)/(period_red)

period_rvi[mask_nan]=np.nan
#Hace un clip para evitar valores extremos. 
period_rvi[period_rvi>1]=np.nan
period_rvi[period_rvi<-1]=np.nan

ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={"rvi": xr.DataArray(period_rvi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units
