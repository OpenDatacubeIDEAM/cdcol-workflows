#Importar las librerÃ­as
import xarray as xr
import numpy as np
#Definir las funciones necesatias para el algoritmo
def isin(element, test_elements, assume_unique=False, invert=False):
    "definiendo la funciÃ³n isin de numpy para la versiÃ³n anterior a la 1.13, en la que no existe"
    element = np.asarray(element)
    return np.in1d(element, test_elements, assume_unique=assume_unique,
                invert=invert).reshape(element.shape)
nbar = xarr0
nodata=-9999
#MÃ¡scara de nubes
validValues=set()
if product=="LS7_ETM_LEDAPS":
    validValues=[66,68,130,132]
elif product == "LS8_OLI_LASRC":
    validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
cloud_mask=isin(nbar["pixel_qa"].values, validValues)

minima={} 
for band in bands:
    #Por cada banda, aplicar la mÃ¡scara de nubes y cambiar el valor de nodata por np.nan
    datos=np.where(np.logical_and(nbar.data_vars[band]!=nodata,cloud_mask),nbar.data_vars[band], np.nan)
    allNan=~np.isnan(datos) #Una mascara que indica quÃ© datos son o no nan. 
    if normalized: #Normalizar, si es necesario.
        #Para cada momento en el tiempo obtener el promedio y la desviaciÃ³n estÃ¡ndar de los valores de reflectancia
        m=np.nanmean(datos.reshape((datos.shape[0],-1)), axis=1)
        st=np.nanstd(datos.reshape((datos.shape[0],-1)), axis=1)
        # usar ((x-xÌ„)/st) para llevar la distribuciÃ³n a media 0 y desviaciÃ³n estÃ¡ndar 1, 
        # y luego hacer un cambio de espacio para la nueva desviaciÃ³n y media. 
        datos=np.true_divide((datos-m[:,np.newaxis,np.newaxis]), st[:,np.newaxis,np.newaxis])*np.nanmin(st)+np.nanmin(m)
    #Calcular la mediana en la dimensiÃ³n de tiempo 
    minima[band]=np.nanmin(datos,0) 
    #Eliminar los valores que no cumplen con el nÃºmero mÃ­nimo de pixeles vÃ¡lidos dado. 
    minima[band][np.sum(allNan,0)<minValid]=np.nan
del datos

import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords)
             for k, v in minima.items()}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})

for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
