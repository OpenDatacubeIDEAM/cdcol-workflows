#!/usr/bin/python3
# coding=utf8
import xarray as xr
import numpy as np
print(xarr0)
nodata=-9999
#medians = {}

banda1=xarr0['hh']
banda2=xarr0['hv']

#CÁLCULO DE 2DO ÍNDICE SPR
s1  = (banda1+banda2)
s2  =(banda1-banda2)
s3 = 2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.cos(banda1-banda2)
s4 =2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.sin(banda1-banda2)
LPR =(s1+s2)/(s1-s2)
SC = 0.5*s1 - 0.5 * s4
OC = 0.5*s1 + 0.5 * s4
CPR =(SC/OC)
m = (((s2**2)+(s3**2)+(s4**2))**(1/2))/s1
print('Indice calculado 1')
period_cpr=[]

period_cpr=CPR[0]

print('Indice calculado')




coordenadas = []
dimensiones =[]
xcords = {}
for coordenada in xarr0.coords:
    if(coordenada != 'time'):
        coordenadas.append( ( coordenada, xarr0.coords[coordenada]) )
        dimensiones.append(coordenada)
        xcords[coordenada] = xarr0.coords[coordenada]


valores = {"cpr": xr.DataArray(period_cpr, dims=dimensiones, coords=coordenadas)}


output = xr.Dataset(valores, attrs={'crs': xarr0.crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0.coords[coordenada].units