# -*- coding: utf-8 -*-


import xarray as xr
import numpy as np
import gc
import collections

import osr

def _tsmi(dataset):
    return (dataset.red.astype('float64') + dataset.green.astype('float64')) * 0.0001 / 2

def create_default_clean_mask(dataset_in):
    """
    Description:
        Creates a data mask that masks nothing.
    -----
    Inputs:
        dataset_in (xarray.Dataset) - dataset retrieved from the Data Cube.
    Throws:
        ValueError - if dataset_in is an empty xarray.Dataset.
    """
    data_vars = dataset_in.data_vars
    if len(data_vars) != 0:
        first_data_var = next(iter(data_vars))
        clean_mask = np.ones(dataset_in[first_data_var].shape).astype(np.bool)
        return clean_mask
    else:
        raise ValueError('`dataset_in` has no data!')


def create_cfmask_clean_mask(cfmask):
    """
    Description:
      Create a clean mask for clear land/water pixels,
      i.e. mask out shadow, snow, cloud, and no data
    -----
    Input:
      cfmask (xarray) - cf_mask from the ledaps products
    Output:
      clean_mask (boolean numpy array) - clear land/water mask
    """

    validValues = set()
    if product == "LS7_ETM_LEDAPS":
        validValues = [66, 68, 130, 132]
    elif product == "LS8_OLI_LASRC":
        validValues = [322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
    clean_mask = np.reshape(np.in1d(cfmask.values.reshape(-1), validValues), cfmask.values.shape)
    return clean_mask

    #########################
    # cfmask values:        #
    #   0 - clear           #
    #   1 - water           #
    #   2 - cloud shadow    #
    #   3 - snow            #
    #   4 - cloud           #
    #   255 - fill          #
    #########################


def tsm(dataset_in, clean_mask=None, no_data=0):
    """
    Inputs:
        dataset_in (xarray.Dataset) - dataset retrieved from the Data Cube.
    Optional Inputs:
        clean_mask (numpy.ndarray with dtype boolean) - true for values user considers clean;
            if user does not provide a clean mask, all values will be considered clean
        no_data (int/float) - no data pixel value; default: -9999
    Throws:
        ValueError - if dataset_in is an empty xarray.Dataset.
    """
    assert 'red' in dataset_in and 'green' in dataset_in, "Red and Green bands are required for the TSM analysis."
    # Default to masking nothing.
    if clean_mask is None:
        clean_mask = create_default_clean_mask(dataset_in)

    tsm = 3983 * _tsmi(dataset_in)**1.6246
    tsm.values[np.invert(clean_mask)] = no_data  # Contains data for clear pixels

    # Create xarray of data
    _coords = { key:dataset_in[key] for key in dataset_in.dims.keys()}
    dataset_out = xr.Dataset({'tsm': tsm}, coords=_coords)
    return dataset_out

#área sobre la cual se hará la consulta:
normalized=True

minValid=1

cfmask = xarr0.pixel_qa
clean_mask = create_cfmask_clean_mask(cfmask)

data =tsm(xarr0, clean_mask=clean_mask, no_data=0)
crs_org = xarr0.crs


output = data

output.attrs["crs"] = crs_org
