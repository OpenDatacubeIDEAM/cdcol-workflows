import xarray as xr
import numpy as np
print "Excecuting medianas v2"
medians={}
nodata=-9999
#Se hace un recorrido sobre las bandas
for band in bands:
    #Los datos se reciben enmascarados
    # 
    #np.isnan verifica que valores no son numero o no estan definidos. En este caso, se usa ~ para invertir los valores y saber que valores de datos si estan definidos
    datos=xarr0.data_vars[band].values
    allNan=~np.isnan(datos)
    # if normalized:
    #     #Calcula la mediana arimetica sobre el arreglo de datos  ,sobre el eje  , ignorando los valores NaN
    #     m=np.nanmean(datos.reshape((datos.shape[0],-1)), axis=1)
    #     #Calcula la desviacion estandar sobre el arreglo de datos  , sobre el eje , ignorando los valores NaN
    #     st=np.nanstd(datos.reshape((datos.shape[0],-1)), axis=1)
    #     datos=np.true_divide((datos-m[:,np.newaxis,np.newaxis]), st[:,np.newaxis,np.newaxis])
    #Calcula la mediana aritmetica 
    medians[band]=np.nanmedian(datos,0).astype(np.int16)
    medians[band][np.sum(allNan,0)<minValid]=nodata
#Elimina la variable datos y la asociacion que tiene en el algoritmo
del datos

import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords)
            for k, v in medians.items()}
#Genera el dataset (netcdf) con las bandas con el sistema de referencia de coordenadas
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})

for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units