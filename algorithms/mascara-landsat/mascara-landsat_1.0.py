#!/usr/bin/python3
# coding=utf8

import numpy as np
print ("Masking " + product)
nodata=-9999

def calculate_mask(valid_values):
    cloud_mask = np.isin(xarr0["pixel_qa"].values, validValues)
    for band in bands:
        xarr0[band].values = np.where(np.logical_and(xarr0.data_vars[band] != nodata, cloud_mask),xarr0.data_vars[band], nodata).astype(np.int16)
    output = xarr0

validValues=set()
if product=="LS7_ETM_LEDAPS" or product == "LS5_TM_LEDAPS":
    validValues=[66,68,130,132]
    calculate_mask(validValues)
elif product == "LS8_OLI_LASRC":
    validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
    calculate_mask(validValues)
elif product=='LS7_ETM_LEDAPS_MOSAIC':
    output = xarr0
else:
    raise Exception("Este algoritmo sólo puede enmascarar LS7_ETM_LEDAPS, LS5_TM_LEDAPS o LS8_OLI_LASRC")
