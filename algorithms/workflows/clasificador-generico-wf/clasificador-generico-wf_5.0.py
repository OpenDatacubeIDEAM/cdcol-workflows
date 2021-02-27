# In[7]:
import xarray as xr
import numpy as np
from joblib import load
import warnings

import os, posixpath

# Preprocesar:
xarrs=list(xarrs.values())
print(type(xarrs))
output0 = xarrs[0]
print(type(output0))
print(f'len xarrs: {len(xarrs)}')
print(f'xarrs: {xarrs}')


print("medianas")
print(f'bandas: {list(output0.data_vars.keys())}')
print(output0)
bands_data=[]


"""
  Revisar las medianas parecen tener inconvenientes, el dato que retorna tiene mas de un slide de tiempo

"""

# Escalar bandas

output0["red"]=output0["red"]/10000
output0["nir"]=output0["nir"]/10000
output0["swir1"]=output0["swir1"]/10000
output0["swir2"]=output0["swir2"]/10000

# Agregar Indices

output0["ndvi"]=(output0["nir"]-output0["red"])/(output0["nir"]+output0["red"])
output0["nbr"] =(output0["nir"]-output0["swir2"])/(output0["nir"]+output0["swir2"])
output0["savi"]=((output0["nir"]-output0["red"])/(output0["nir"]+output0["red"] + (0.5)))*(1+0.5)

print(f'Data + indices: {output0}')


for band in output0.data_vars.keys():
    # pixel_qa is removed from xarr0 by Compuesto Temporal de Medianas
    if band != 'pixel_qa':
        bands_data.append(output0[band])
    print(f'band processed: {band}')

bands_data = np.dstack(bands_data)


rows, cols, n_bands = bands_data.shape

n_samples = rows*cols
flat_pixels = bands_data.reshape((n_samples, n_bands))

where_are_NaNs = np.isnan(flat_pixels)
print(f'NaNs in data : {where_are_NaNs.sum()}')
flat_pixels[where_are_NaNs] = -9999

where_are_Infs = np.isinf(flat_pixels)
print(f'Infs in data : {where_are_Infs .sum()}')
flat_pixels[where_are_Infs] = -9999


print(f'Flat_pixels shape: {flat_pixels.shape}')
print(f'Flat_pixels: {flat_pixels}')

# Cargar modelos

# Clasificacion de los 10 mapas
y_maps=[]

print(os.path.dirname(folder))
print('Cargar los modelos')
for i in range(10):
    clf_load = load(posixpath.join(os.path.dirname(os.path.dirname(folder)),'random-forest-training_6.0/models/modelo_'+str(i)+'.joblib'))
    print(f'Cargado modelo {i}')
    y_maps.append(clf_load.predict(flat_pixels))

# Generar 10 imagenes
kClasificaciones=[]
for i in range(10):
    y_maps[i][where_are_NaNs[:,0]]=np.nan
    classification = y_maps[i].reshape((rows, cols))
    kClasificaciones.append(classification)


# union de mapas con la mediana

result = np.apply_along_axis(np.mean,0,kClasificaciones)

print('result classification',result)


coordenadas = []
dimensiones = []
xcords = {}
for coordenada in xarrs[0].coords:
    if (coordenada != 'time'):
        coordenadas.append((coordenada, xarrs[0].coords[coordenada]))
        dimensiones.append(coordenada)
        xcords[coordenada] = xarrs[0].coords[coordenada]

valores = {"classified": xr.DataArray(result, dims=dimensiones, coords=coordenadas)}
#array = xr.DataArray(result, dims=dimensiones, coords=coordenadas)
#array.astype('float32')
#valores = {"classified": array}
print('creacion mapa biomasa')


biomasa = xr.Dataset(valores, attrs={'crs': xarrs[0].crs})
for coordenada in output0.coords:
    biomasa.coords[coordenada].attrs["units"] = xarrs[0].coords[coordenada].units
print('creacion mapa carbono')

carbono = biomasa.copy()*0.47

print('preparacion salidas')
outputs = {'biomasa': biomasa,
'carbono': carbono }
print(f'outputs:{outputs}')


classified = biomasa.classified
classified.values = classified.values.astype('float32')

