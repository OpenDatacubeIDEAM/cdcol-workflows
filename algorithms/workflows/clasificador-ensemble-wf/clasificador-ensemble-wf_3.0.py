# In[7]:
import xarray as xr
import numpy as np
from sklearn.externals import joblib
import warnings

# In[21]:

# Preprocesar:
nmed=None
nan_mask=None
xarrs=list(xarrs.values())
print(type(xarrs))
medians1 = xarrs[0]
print(type(medians1))
print("medianas")
bands_data = []

print('medians1.datavars',medians1.data_vars.keys())


###test 2
bands_data=[]

bands2=list(medians1.data_vars.keys())
print('bands2',bands2)
#print('mediansband',medians1[band])

for band in bands2: 
	if band != 'pixel_qa':
   	 bands_data.append(medians1[band])
bands_data = np.dstack(bands_data)

print('bands_data_test2',bands_data)


rows, cols, n_bands = bands_data.shape

print('rows',rows)
print('cols',cols)

print('n_bands',n_bands)



print('fin test2')

print('inicio test3')

n_samples = rows*cols


flat_pixels = bands_data.reshape((n_samples, n_bands))
#mascara valores nan por valor no data
mask_nan=np.isnan(flat_pixels)
flat_pixels[mask_nan]=-9999

np.isfinite(flat_pixels)
_msk=np.sum(np.isfinite(flat_pixels),1)>1
flat_pixels= flat_pixels[_msk,:]
#training_labels=training_labels[_msk]


#result = bagging_clf.predict(flat_pixels)
#classification = result.reshape((rows, cols))

print('fin test3')


import os

print("modelo")

model = None
for file in other_files:
    if file.endswith(".pkl"):
        model = file
        break
if model is None:
    raise "Debería haber un modelo en la carpeta " + modelos

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=UserWarning)
    eclf = joblib.load(model)
    print(eclf)

print(eclf)
print("clasificacion final voting")
result = eclf.predict(flat_pixels)
result = result.reshape((rows, cols))
print("fin funcion de clasificacion")


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

output = xr.Dataset(valores, attrs={'crs': xarrs[0].crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarrs[0].coords[coordenada].units

classified = output.classified
classified.values = classified.values.astype('float32')
