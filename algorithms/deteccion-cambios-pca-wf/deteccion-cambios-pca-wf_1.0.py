from matplotlib.mlab import PCA
from sklearn.preprocessing import normalize
from scipy.cluster.vq import kmeans2,vq
import xarray as xr
import numpy as np

medians = xarrs[0]
medians = xarrs[1]
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
