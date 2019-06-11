#!/usr/bin/python3
# coding=utf8
import datacube
import numpy as np
#from datacube.storage import netcdf_writer
#from datacube.model import Variable
from datacube.drivers.netcdf import writer as netcdf_writer
from datacube.utils.geometry import CRS
import os
import re
import xarray as xr
import itertools
import rasterio
import time
import logging

logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
logging.getLogger().addHandler(logging.StreamHandler())

ALGORITHMS_FOLDER = "/web_storage/algorithms/workflows"
COMPLETE_ALGORITHMS_FOLDER="/web_storage/algorithms"
RESULTS_FOLDER = "/web_storage/results"
LOGS_FOLDER = "/web_storage/logs"
nodata=-9999

def saveNC(output,filename,history):
    start = time.time()
    nco=netcdf_writer.create_netcdf(filename)
    nco.history = (history.encode('ascii','replace'))

    coords=output.coords
    cnames=()
    for x in coords:
        if not 'units' in coords[x].attrs:
            if x == "time":
                coords[x].attrs["units"]=u"seconds since 1970-01-01 00:00:00"
        netcdf_writer.create_coordinate(nco, x, coords[x].values, coords[x].units)
        cnames=cnames+(x,)
    _crs=output.crs
    if isinstance(_crs, xr.DataArray):
        _crs=CRS(str(_crs.spatial_ref))
    netcdf_writer.create_grid_mapping_variable(nco, _crs)
    for band in output.data_vars:
        #Para evitar problemas con xarray <0.11
        if band in coords.keys() or band == 'crs':
            continue
        output.data_vars[band].values[np.isnan(output.data_vars[band].values)]=nodata
        var= netcdf_writer.create_variable(nco, band, netcdf_writer.Variable(output.data_vars[band].dtype, nodata, cnames, None) ,set_crs=True)
        var[:] = netcdf_writer.netcdfy_data(output.data_vars[band].values)
    nco.close()

    end = time.time()
    logging.info('TIEMPO SALIDA NC:' + str((end - start)))

def readNetCDF(file):
    start = time.time()
    try:
        _xarr=xr.open_dataset(file, mask_and_scale=True)
        if _xarr.data_vars['crs'] is not None:
            _xarr.attrs['crs']= _xarr.data_vars['crs']
            _xarr = _xarr.drop('crs')
        end = time.time()
        logging.info('TIEMPO CARGA NC:' + str((end - start)))
        return _xarr
    except Exception as e:
        logging.info('ERROR CARGA NC:' + str(e))


def getUpstreamVariable(task, context,key='return_value'):
    start = time.time()
    task_instance = context['task_instance']
    upstream_tasks = task.get_direct_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    #upstream_task_ids = task.get_direct_relatives(upstream=True)
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key=key)
    end = time.time()
    logging.info('TIEMPO UPSTREAM:' + str((end - start)))
    return list(itertools.chain.from_iterable(filter(None.__ne__,upstream_variable_values)))

def _get_transform_from_xr(dataset):
    """Create a geotransform from an xarray dataset.
    """

    from rasterio.transform import from_bounds
    geotransform = from_bounds(dataset.longitude[0], dataset.latitude[-1], dataset.longitude[-1], dataset.latitude[0],
                               len(dataset.longitude), len(dataset.latitude))
    return geotransform

def write_geotiff_from_xr(tif_path, dataset, bands=[], no_data=-9999, crs="EPSG:4326"):
    """Write a geotiff from an xarray dataset.

    Args:
        tif_path: path for the tif to be written to.
        dataset: xarray dataset
        bands: list of strings representing the bands in the order they should be written
        no_data: nodata value for the dataset
        crs: requested crs.

    """
    print(dataset.data_vars.keys())
    print(type(dataset.data_vars.keys()))
    print(list(dataset.data_vars.keys()))
    bands=list(dataset.data_vars.keys())
    assert isinstance(bands, list), "Bands must a list of strings"
    assert len(bands) > 0 and isinstance(bands[0], str), "You must supply at least one band."
    with rasterio.open(
            tif_path,
            'w',
            driver='GTiff',
            height=dataset.dims['latitude'],
            width=dataset.dims['longitude'],
            count=len(bands),
            dtype=dataset[bands[0]].dtype,#str(dataset[bands[0]].dtype),
            crs=crs,
            transform=_get_transform_from_xr(dataset),
            nodata=no_data) as dst:
        for index, band in enumerate(bands):
            dst.write_band(index + 1, dataset[band].values.astype(dataset[bands[0]].dtype), )
            tag = ["{}={}".format('Band'+str(index+1), bands[index])]
            dst.update_tags("{}={}".format('Band'+str(index+1), bands[index]))
            dst.set_band_description(index + 1, band)
        dst.close()