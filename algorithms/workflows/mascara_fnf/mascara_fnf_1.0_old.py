import sys
import numpy as np
import time
from arrnorm.auxil.auxil import similarity
#import matplotlib.pyplot as plt
from scipy import stats
from arrnorm.auxil.auxil import orthoregress
from operator import itemgetter
from scipy import linalg, stats
import arrnorm.auxil.auxil as auxil
from datetime import datetime
import xarray as xr


# Preprocesar:
nmed=None
nan_mask=None
print('values del directorio')
xarr0=list(xarrs.values())
print(type(xarrs))

#mosaico de LS8

print(xarrs.keys())
inDataset1=[xarrs[k] for k in xarrs.keys() if 'clasificador' in k][0];
#inDataset1.sortby('latitude', ascending=False)
inDataset1 = inDataset1.sortby('latitude', ascending=False)

print('plot_objetivo_inDataset1')
print(inDataset1)

#consulta de mosaico
inDataset2=[xarrs[k] for k in xarrs.keys() if 'consulta_referencia' in k][0];
print('plot_referencia_inDataset2')
print(inDataset2)
print(type(inDataset2))

print(inDataset2["fnf_mask"].values[0]==1)
fnf_mas=np.isin(inDataset2["fnf_mask"].values[0]==1, np.nan)
print('la mascara es tipo')
print(type(fnf_mas))
print(fnf_mas)

fnf_mask_tmp=xr.DataArray(inDataset2["fnf_mask"])

fnf_mask=np.isin(fnf_mask_tmp.values[0]==1, np.nan)
print('la mascara temporal 2 es tipo')
print(type(fnf_mask_tmp))
print(fnf_mask)

print('plot_tipo_objetivo_inDataset1')
print(type(inDataset1))

inDataset12=xr.DataArray(inDataset1["classified"])


ImgResultadof=np.where(fnf_mask_tmp.values[0]==1, np.nan,inDataset12)

print(ImgResultadof)
output=ImgResultadof

print("termino funcion")
#print(type(inDataset1[0]))
print("asignacion de dataset")
xarrs[0]=inDataset1




ncoords=[]
xdims =[]
xcords={}
for x in xarrs[0].coords:
    if(x!='time'):
        ncoords.append( ( x, xarrs[0].coords[x]) )
        xdims.append(x)
        xcords[x]=xarrs[0].coords[x]
variables ={"mask": xr.DataArray(output, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':xarrs[0].crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarrs[0].coords[x].units

print("clasificacion_mask")
#print(output)

