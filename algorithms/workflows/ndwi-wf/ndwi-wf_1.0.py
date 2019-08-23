import xarray as xr
import numpy as np
print("Indice de agua de diferencia normalizada")
	
period_green = xarr0["green"].values
period_nir = xarr0["nir"].values
mask_nan=np.logical_or(np.isnan(period_green), np.isnan(period_nir ))


period_ndwi = (period_green-period_nir )/(period_green+period_nir )

period_ndwi[mask_nan]=np.nan

#Hace un clip para evitar valores extremos.
# Comentado por Aurelio
#period_ndwi[period_ndwi>1]=1.1
#period_ndwi[period_ndwi<-1]=-1.1
period_ndwi[period_ndwi>1]=np.nan
period_ndwi[period_ndwi<-1]=np.nan


ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={"ndwi": xr.DataArray(period_ndwi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units
