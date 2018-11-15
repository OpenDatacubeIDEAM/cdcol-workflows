#Requiere que el producto sea LS7_ETM_LEDAPS, LS5_TM_LEDAPS o LS8_OLI_LASRC
import xarray as xr
import numpy as np
print "Masking "+product

#La funcion isin permite verificar si el elemento (elemento) que se pasa por parametro está dentro del arreglo (test_elements)
#Esta funcion se define para verificar los valores validos de la mascara de nube (pixel_qa) 
def isin(element, test_elements, assume_unique=False, invert=False):
    "definiendo la función isin de numpy para la versión anterior a la 1.13, en la que no existe"
    element = np.asarray(element)
    return np.in1d(element, test_elements, assume_unique=assume_unique,
                invert=invert).reshape(element.shape)
#Un valor negativo suficientemente grande para representar cuando no hay datos
nodata=-9999
medians={}

#Conjunto de valores valido para la mascara de nube
validValues=set()

#Dependiendo del satellite, se definen unos valores distintos para la mascara
#Estos valores se definen en la página de LandSat: https://landsat.usgs.gov/landsat-surface-reflectance-quality-assessment
#Son valores validos porque representan agua (water) o un "pixel claro" (clear)
if product=="LS7_ETM_LEDAPS" or product == "LS5_TM_LEDAPS":
    validValues=[66,68,130,132]
elif product == "LS8_OLI_LASRC":
    validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
else:
    raise Exception("Este algoritmo sólo puede enmascarar LS7_ETM_LEDAPS, LS5_TM_LEDAPS o LS8_OLI_LASRC")

cloud_mask=isin(xarr0["pixel_qa"].values, validValues)
for band in bands:
    # np.where es la funcion where de la libreria numpy que retorna un arreglo o un set de arreglos con datos que cumplen con la condicion dada por parametro
    # 
    xarr0[band].values=np.where(np.logical_and(xarr0.data_vars[band]!=nodata,cloud_mask),xarr0.data_vars[band], np.nan).astype(np.int16)

output=xarr0