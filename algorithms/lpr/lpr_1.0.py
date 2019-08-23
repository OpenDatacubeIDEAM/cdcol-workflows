# -*- coding: utf-8 -*-

__autor__ = "Miguel Angel Cañon Ramos"
__credits__ = ["none"]
__version__ = "1,0"
__correo__ = "Miguelca27@gmail.com - miguel_canon@javeriana.edu.co"
__status__ = "en desarrollo"

import xarray as xr
import numpy as np
from osgeo import gdal, ogr
import sys
import os
from osgeo import osr
import pandas as pd


nbar = xarr0
nodata = -9999
medians1 = {}


Bandas=["hh","hv"]
for band in Bandas[0:]:
    datos = nbar.data_vars[band]
    allNan = ~np.isnan(datos)
    medians1[band] = datos



banda1 = medians1['hh']
banda2 = medians1['hv']



s1  = np.add(banda1,banda2)
s2  =np.subtract(banda1,banda2)
s3 = 2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.cos(np.subtract(banda1,banda2))
s4 =2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.sin(np.subtract(banda1,banda2))


LPR =np.true_divide(np.add(s1,s2),np.subtract(s1,s2))
SC = 0.5*s1 - 0.5 * s4
OC = 0.5*s1 + 0.5 * s4

CPR =np.true_divide(SC,OC)
m = (((s2**2)+(s3**2)+(s4**2))**(1/2))/s1


coordenadas = []
dimensiones =[]
xcords = {}
for coordenada in xarr0.coords:
    if(coordenada != 'time'):
        coordenadas.append( ( coordenada, xarr0.coords[coordenada]) )
        dimensiones.append(coordenada)
        xcords[coordenada] = xarr0.coords[coordenada]


valores = {"LPR": xr.DataArray(LPR[0], dims=dimensiones, coords=coordenadas)}
valores["CPR"]=xr.DataArray(CPR[0], dims=dimensiones, coords=coordenadas)

output = xr.Dataset(valores, attrs={'crs': xarr0.crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0.coords[coordenada].units
