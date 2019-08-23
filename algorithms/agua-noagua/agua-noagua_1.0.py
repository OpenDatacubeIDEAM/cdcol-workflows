# -*- coding: utf-8 -*-

import xarray as xr
import numpy as np
print "Excecuting forest_noforest v1 "

def isin(element, test_elements, assume_unique=False, invert=False):
    "definiendo la función isin de numpy para la versión anterior a la 1.13, en la que no existe"
    element = np.asarray(element)
    return np.in1d(element, test_elements, assume_unique=assume_unique, invert=invert).reshape(element.shape)


########################################################################
# directorio = 'D:\Dropbox\Python Scripts\Data_Cube_IDEAM\Tiles/'
# xarr0=xr.open_dataset(directorio+"query_just-query_2_-68.5_(2013-01-012013-12-31)_output.nc")
#
#
# product = 'LS8_OLI_LASRC' #Producto sobre el que se hará la consulta (unidad de almacenamiento)
#
# #área sobre la cual se hará la consulta:
# min_long = -69
# min_lat = 2
normalized=True

slice_size = 1

ndwi_threshold = ndwi_threshold 

#vegetation_rate =vegetation_rate

#########################################################################

minValid=1
nbar = xarr0
nodata=-9999
medians={}
bands=["green","nir"]

validValues=set()
if product=="LS7_ETM_LEDAPS" or product=="LS5_TM_LEDAPS":
    validValues=[66,68,130,132]
elif product == "LS8_OLI_LASRC":
    validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]

cloud_mask=isin(nbar["pixel_qa"].values, validValues)

for band in bands:
    datos=np.where(np.logical_and(nbar.data_vars[band]!=nodata,cloud_mask),nbar.data_vars[band], np.nan)
    allNan=~np.isnan(datos)
    if normalized:
        m=np.nanmean(datos.reshape((datos.shape[0],-1)), axis=1)
        st=np.nanstd(datos.reshape((datos.shape[0],-1)), axis=1)
        datos=np.true_divide((datos-m[:,np.newaxis,np.newaxis]), st[:,np.newaxis,np.newaxis])*np.nanmean(st)+np.nanmean(m)
    medians[band]=np.nanmedian(datos,0)
    medians[band][np.sum(allNan,0)<minValid]=np.nan
del datos

period_green = medians["green"]
period_nir = medians["nir"]
del medians

mask_nan=np.logical_or(np.isnan(period_green), np.isnan(period_nir))
period_ndwi = np.true_divide( np.subtract(period_green,period_nir) , np.add(period_green,period_nir))



period_ndwi[mask_nan]=np.nan
#Hace un clip para evitar valores extremos.
period_ndwi[period_ndwi>1]=np.nan
period_ndwi[period_ndwi<-1]=np.nan




height = period_ndwi.shape[0]
width = period_ndwi.shape[1]



agua_noagua =np.full(period_ndwi.shape, -1)
for y1 in xrange(0, height, slice_size):
    for x1 in xrange(0, width, slice_size):
        x2 = x1 + slice_size
        y2 = y1 + slice_size
        if(x2 > width):
            x2 = width
        if(y2 > height):
            y2 = height
        submatrix = period_ndwi[y1:y2,x1:x2]
        ok_pixels = np.count_nonzero(~np.isnan(submatrix))
        if ok_pixels==0:
            agua_noagua[y1:y2,x1:x2] = nodata
        elif float(np.nansum(submatrix>ndwi_threshold)):#/float(ok_pixels) >= vegetation_rate :
            agua_noagua[y1:y2,x1:x2] = 1
        else:
            agua_noagua[y1:y2,x1:x2] = 0


ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"bosque_nobosque": xr.DataArray(agua_noagua.astype(np.int8), dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
print output
