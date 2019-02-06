import xarray as xr
import numpy as np

nbar = xarr0
nodata=-9999


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
    medians = {}
    if product == "LS7_ETM_LEDAPS" or product == "LS5_TM_LEDAPS":
        validValues = [66, 68, 130, 132]
    elif product == "LS8_OLI_LASRC":
        validValues = [322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
    
    cloud_mask=np.isin(nbar["pixel_qa"].values, validValues)
    print("cloud_mask")
    print(cloud_mask)
    for band in bands:
        datos = np.where(np.logical_and(nbar.data_vars[band] != nodata, cloud_mask), nbar.data_vars[band], np.nan)
        allNan = ~np.isnan(datos)
        if normalized:
            m = np.nanmean(datos.reshape((datos.shape[0], -1)), axis=1)
            print("m")
            print(m)
            st = np.nanstd(datos.reshape((datos.shape[0], -1)), axis=1)
            print("st")
            print(st)
            datos2 = ((datos - m[:, np.newaxis, np.newaxis])/st[:, np.newaxis, np.newaxis])
            datos=(datos2)* np.nanmean(st) + np.nanmean(m)
            print("datos2")
            print(datos)
        medians[band] = np.nanmedian(datos, 0)
        print("medians")
        print(medians[band])
        medians[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos
    


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
variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords)
             for k, v in medians.items()}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
