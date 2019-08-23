# -*- coding: utf-8 -*-

from matplotlib.mlab import PCA
from sklearn.preprocessing import normalize
from scipy.cluster.vq import kmeans2, vq
import xarray as xr
import numpy as np
from osgeo import gdal, ogr
import sys
import os
from osgeo import osr
import pandas as pd


# Calcular el compuesto de medianas para cada uno de las entradas

def isin(element, test_elements, assume_unique=False, invert=False):
    "definiendo la función isin de numpy para la versión anterior a la 1.13, en la que no existe"
    element = np.asarray(element)
    return np.in1d(element, test_elements, assume_unique=assume_unique, invert=invert).reshape(element.shape)


nbar = xarr0
nodata = -9999
medians1 = {}


Bandas=["vh","vv"]
for band in Bandas[0:]:
    datos = nbar.data_vars[band]
    allNan = ~np.isnan(datos)
    medians1[band] = datos

del datos
nbar = xarr1
nodata = -9999
medians2 = {}

for band in Bandas[0:]:
    datos = nbar.data_vars[band]
    allNan = ~np.isnan(datos)

    medians2[band] = datos

del datos
# Preprocesar:
nmed = None
nan_mask = None
for band in medians1:
    b = np.asarray(medians1[band]).ravel()
    if nan_mask is None:
        nan_mask = np.isnan(b)
    else:
        nan_mask = np.logical_or(nan_mask, np.isnan(np.asarray(medians1[band]).ravel()))
    b[np.isnan(b)] = np.nanmedian(b)
    if nmed is None:
        sp = medians1[band].shape
        nmed = b
    else:
        nmed = np.vstack((nmed, b))
    c = np.asarray(medians2[band]).ravel()
    nan_mask = np.logical_or(nan_mask, np.isnan(c))
    c[np.isnan(c)] = np.nanmedian(c)
    nmed = np.vstack((nmed, c))
del medians1
del medians2
# PCA
r_PCA = PCA(nmed.T)
salida = r_PCA.Y.T.reshape((r_PCA.Y.T.shape[0],) + sp)
# Kmeans - 4 clases
km_centroids, kmvalues = kmeans2(r_PCA.Y, 4)
# Salida:
salida[:, nan_mask.reshape(sp)] = np.nan

ncoords = []
xdims = []
xcords = {}
for x in xarr0.coords:
    if (x != 'time'):
        ncoords.append((x, xarr0.coords[x]))
        xdims.append(x)
        xcords[x] = xarr0.coords[x]

valores = {}
i = 1



for x in salida:
    valores["pc" + str(i)] = xr.DataArray(x[0], dims=xdims, coords=ncoords)
    i += 1

output = xr.Dataset(valores, attrs={'crs': xarr0.crs})


for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0.coords[coordenada].units
