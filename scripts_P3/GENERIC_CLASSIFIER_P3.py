import xarray as xr
import numpy as np
from sklearn.externals import joblib
import os

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
    for band in bands:
        datos = np.where(np.logical_and(nbar.data_vars[band] != nodata, cloud_mask), nbar.data_vars[band], np.nan)
        allNan = ~np.isnan(datos)
        if normalized:
            m = np.nanmean(datos.reshape((datos.shape[0], -1)), axis=1)
            st = np.nanstd(datos.reshape((datos.shape[0], -1)), axis=1)
            datos2 = ((datos - m[:, np.newaxis, np.newaxis])/st[:, np.newaxis, np.newaxis])
            datos=(datos2)* np.nanmean(st) + np.nanmean(m)
        medians[band] = np.nanmedian(datos, 0)
        medians[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos
    


#Preprocesar:
nmed=None
nan_mask=None
for band in bands:
    b=medians1[band].ravel()
    if nan_mask is None:
        nan_mask=np.isnan(b)
    else:
        nan_mask=np.logical_or(nan_mask, np.isnan(medians1[band].ravel()))
    b[np.isnan(b)]=np.nanmedian(b)
    if nmed is None:
        sp=medians1[band].shape
        nmed=b
    else:
        nmed=np.vstack((nmed,b))



model = None
for file in os.listdir(modelo):
    print file
    if file.endswith(".pkl"):
        model = file
        break
print model
if model is None:
    raise "Debería haber un modelo en la carpeta " + modelo

classifier = joblib.load(os.path.join(modelo, model))
result = classifier.predict(nmed.T)
result = result.reshape(sp)

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
variables ={"classified": xr.DataArray(result, dims=xdims,coords=ncoords)
            
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
