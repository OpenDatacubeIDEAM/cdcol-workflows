
import xarray as xr
import numpy as np


nbar = xarr0
nodata=-9999
bands=["red","nir"]


cvalidValues=set()
if product=='LS7_ETM_LEDAPS_MOSAIC':
    medians={}
    for band in bands[0:]:
        datos = xarr0.data_vars[band]
        allNan = ~np.isnan(datos)
        medians[band] = datos
        medians[band] = np.nanmedian(datos, 0)
        medians[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos

else:
   
    if product == "LS7_ETM_LEDAPS" or product == "LS5_TM_LEDAPS":
        validValues = [66, 68, 130, 132]
    elif product == "LS8_OLI_LASRC":
        validValues = [322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
    
    cloud_mask=np.isin(nbar["pixel_qa"].values, validValues)



datos_red=np.where(np.logical_and(nbar.data_vars["red"]!=nodata,cloud_mask),nbar.data_vars["red"], np.nan)
datos_nir=np.where(np.logical_and(nbar.data_vars["nir"]!=nodata,cloud_mask),nbar.data_vars["nir"], np.nan)

ndviTotal = []
for i in range(0,len(datos_red)):
    image1red = datos_red[i]
    image1nir = datos_nir[i]
    allNan=~np.isnan(image1red)
    mask_nan = np.logical_or(np.isnan(image1red),np.isnan(image1nir))
    period_nvdi = (image1nir-image1red)/(image1nir+image1red)
    period_nvdi[mask_nan] = np.nan
    ndviTotal.append(period_nvdi)
del datos_red,datos_nir


max_ndvi = np.nanmax(ndviTotal,0)
max_ndvi[max_ndvi>1]=np.nan
max_ndvi[max_ndvi<-1]=np.nan


# > **Asignación de coordenadas**  

import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"ndvi": xr.DataArray(max_ndvi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units

