#Parámetros: bands, modelos
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
medians1 = xarrs[0]
for band in medians1.data_vars.keys():
    if band == "crs":
        continue
    b=np.ravel(medians1.data_vars[band].values)
    if nan_mask is None:
        nan_mask=np.isnan(b)
    else:
        nan_mask=np.logical_or(nan_mask, np.isnan(medians1.data_vars[band].values.ravel()))
    b[np.isnan(b)]=np.nanmedian(b)
    if nmed is None:
        sp=medians1.data_vars[band].values.shape
        nmed=b
    else:
        nmed=np.vstack((nmed,b))

# In[12]:

import os

model = None
for file in other_files:
    if file.endswith(".pkl"):
        model = file
        break
if model is None:
    raise "Debería haber un modelo en la carpeta " + modelos

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=UserWarning)
    classifier = joblib.load(model)
result = classifier.predict(nmed.T)
result = result.reshape(sp)

# In[ ]:


# In[24]:

coordenadas = []
dimensiones = []
xcords = {}
for coordenada in xarrs[0].coords:
    if (coordenada != 'time'):
        coordenadas.append((coordenada, xarrs[0].coords[coordenada]))
        dimensiones.append(coordenada)
        xcords[coordenada] = xarrs[0].coords[coordenada]
valores = {"classified": xr.DataArray(result, dims=dimensiones, coords=coordenadas)}
output = xr.Dataset(valores, attrs={'crs': xarrs[0].crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarrs[0].coords[coordenada].units
