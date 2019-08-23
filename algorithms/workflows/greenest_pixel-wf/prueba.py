import xarray as xr
import numpy as np
print("Indice de vegetacion de diferencia normalizada")

datos_red = xarr0["red"].values
datos_nir = xarr0["nir"].values
print(datos_red[1])

period_red=[]
period_ndvi=[]

print(len(datos_red))

ndviTotal = []
for i in range(0,len(datos_red)):
    period_red = datos_red[i]
    period_nir = datos_nir[i]
    allNan=~np.isnan(period_red)
    mask_nan=np.logical_or(np.isnan(period_red), np.isnan(period_nir))
    period_nvdi = (period_nir-period_red)/ (period_nir+period_red)
    period_nvdi[mask_nan] = np.nan
    ndviTotal.append(period_nvdi)
del datos_red,datos_nir

max_ndvi = []

max_ndvi = np.nanmax(ndviTotal,0)
max_ndvi[max_ndvi>1]=np.nan
max_ndvi[max_ndvi<-1]=np.nan


ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={"max_ndvi": xr.DataArray(max_ndvi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units

