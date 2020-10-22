#!/usr/bin/python3
# coding=utf8
import xarray as xr
import numpy as np
print(xarr0)
nodata=-9999
#medians = {}
time_axis = list(xarr0.coords.keys()).index('time')

print(xarr0)
print(type(xarr0))


banda1=xarr0['hh']
banda2=xarr0['hv']

#Cálculo del 1er RDFI índice Radar Forest Degradation  para ALOS PALSAR
rdfi =(banda1-banda2)/(banda1+banda2)
print('Indice calculado')


coordenadas = []
dimensiones =[]
xcords = {}
for coordenada in xarr0.coords:
    if(coordenada != 'time'):
        coordenadas.append( ( coordenada, xarr0.coords[coordenada]) )
        dimensiones.append(coordenada)
        xcords[coordenada] = xarr0.coords[coordenada]


valores = {"RDFI": xr.DataArray(rdfi[0], dims=dimensiones, coords=coordenadas)}


output = xr.Dataset(valores, attrs={'crs': xarr0.crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0.coords[coordenada].units