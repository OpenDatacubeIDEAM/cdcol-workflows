# coding=utf8
import datacube
import numpy as np
from datacube.drivers.netcdf import writer as netcdf_writer
from datacube.model import CRS
import os
import re
import xarray as xr
import itertools

import time
import logging

logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
logging.getLogger().addHandler(logging.StreamHandler())

ALGORITHMS_FOLDER = "/web_storage/algorithms"
RESULTS_FOLDER = "/web_storage/results"
nodata=-9999
def saveNC(output,filename, history):
    start = time.time()
    nco=netcdf_writer.create_netcdf(filename)
    nco.history = (history.decode('utf-8').encode('ascii','replace'))

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
        _xarr=xr.open_dataset(file, mask_and_scale=False)
    except Exception as e:
        logging.info('CARGA NC EXCEPTION:' + str(e)) 
    # _xarr=xr.open_dataset(file)
    end = time.time()
    logging.info('TIEMPO CARGA NC:' + str((end - start)))
    return _xarr

def getUpstreamVariable(task, context,key='return_value'):
    start = time.time()
    task_instance = context['task_instance']
    upstream_tasks = task.get_direct_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key=key)
    end = time.time()
    logging.info('TIEMPO UPSTREAM:' + str((end - start)))
    return list(itertools.chain.from_iterable(upstream_variable_values))