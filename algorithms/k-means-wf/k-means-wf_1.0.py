from matplotlib.mlab import PCA
from sklearn.preprocessing import normalize
from scipy.cluster.vq import kmeans2,vq
import xarray as xr
import numpy as np
#Preprocesar:
nmed=None
nan_mask=None
medians1 = xarr0
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
del medians1

#PCA
r_PCA=PCA(nmed.T)
salida= r_PCA.Y.T.reshape((r_PCA.Y.T.shape[0],)+sp)
#Kmeans - 4 clases
km_centroids, kmvalues=kmeans2(r_PCA.Y,classes)
#Salida:
salida[:,nan_mask.reshape(sp)]=np.nan
kmv= kmvalues.T.reshape(sp)
kmv[nan_mask.reshape(sp)]=nodata
coordenadas = []
dimensiones =[]
xcords = {}
for coordenada in xarr0.coords:
    if(coordenada != 'time'):
        coordenadas.append( ( coordenada, xarr0.coords[coordenada]) )
        dimensiones.append(coordenada)
        xcords[coordenada] = xarr0.coords[coordenada]
valores = {"kmeans": xr.DataArray(kmv, dims=dimensiones, coords=coordenadas)}
#Genera el dataset (netcdf) con las bandas con el sistema de referencia de coordenadas
output = xr.Dataset(valores, attrs={'crs': xarr0.crs})

for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0.coords[coordenada].units