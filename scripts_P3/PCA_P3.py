
from matplotlib.mlab import PCA
from sklearn.preprocessing import normalize
from scipy.cluster.vq import kmeans2,vq
import xarray as xr
import numpy as np



nbar = xarr0
nodata=-9999


cvalidValues=set()
if product=='LS7_ETM_LEDAPS_MOSAIC':
    medians1={}
    for band in bands[0:]:
        datos = xarr0.data_vars[band]
        allNan = ~np.isnan(datos)
        medians1[band] = datos
        medians1[band] = np.nanmedian(datos, 0)
        medians1[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos

else:
    medians1 = {}
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
        medians1[band] = np.nanmedian(datos, 0)
        medians1[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos
    


nbar = xarr1
nodata=-9999
medians2={}


cvalidValues=set()
if product=='LS7_ETM_LEDAPS_MOSAIC':
    medians2={}
    for band in bands[0:]:
        datos = xarr0.data_vars[band]
        allNan = ~np.isnan(datos)
        medians2[band] = datos
        medians2[band] = np.nanmedian(datos, 0)
        medians2[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos

else:
    medians2 = {}
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
        medians2[band] = np.nanmedian(datos, 0)
        medians2[band][np.sum(allNan, 0) < minValid] = np.nan
    del datos
    

#Preprocesar:
nmed=None
nan_mask=None
for band in medians1:
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
    c=medians2[band].ravel()
    nan_mask=np.logical_or(nan_mask, np.isnan(c))
    c[np.isnan(c)]=np.nanmedian(c)
    nmed=np.vstack((nmed,c))
del medians1
del medians2

#PCA
r_PCA=PCA(nmed.T)
salida= r_PCA.Y.T.reshape((r_PCA.Y.T.shape[0],)+sp)
#Kmeans - 4 clases
km_centroids, kmvalues=kmeans2(r_PCA.Y,4)
#Salida:
salida[:,nan_mask.reshape(sp)]=np.nan
kmv= kmvalues.T.reshape(sp)
kmv[nan_mask.reshape(sp)]=nodata



import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"kmeans": xr.DataArray(kmv, dims=xdims, coords=xcords)}
i=1
for x in salida:
    variables["pc"+str(i)]=xr.DataArray(x, dims=xdims, coords=xcords)
    i+=1
output = xr.Dataset(variables, attrs={'crs': xarr0.crs})
#output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units