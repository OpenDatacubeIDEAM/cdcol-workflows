import numpy as np
print(product)
print ("Masking " + product['name'])
nodata=-9999
validValues=set()
if product['name']=="LS7_ETM_LEDAPS" or product['name'] == "LS5_TM_LEDAPS":
    validValues=[66,68,130,132,80, 112, 144, 176]
elif product['name'] == "LS8_OLI_LASRC":
    validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348,336, 368, 400, 432, 848, 880, 912, 944, 1352]
else:
    raise Exception("Este algoritmo sólo puede enmascarar LS7_ETM_LEDAPS, LS5_TM_LEDAPS o LS8_OLI_LASRC")

cloud_mask = np.isin(xarr0["pixel_qa"].values, validValues)
for band in product['bands']:
    print("entra a enmascarar")
    xarr0[band].values = np.where(np.logical_and(xarr0.data_vars[band] != nodata, cloud_mask), xarr0.data_vars[band], -9999)
output = xarr0