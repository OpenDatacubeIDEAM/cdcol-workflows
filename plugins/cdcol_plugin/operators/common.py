# coding=utf8
import datacube
import numpy as np
from datacube.storage import netcdf_writer
from datacube.model import Variable, CRS
import os
import re
import xarray as xr
import itertools
ALGORITHMS_FOLDER = "/web_storage/algorithms"
RESULTS_FOLDER = "/web_storage/results"
nodata=-9999
def saveNC(output,filename, history):

    nco=netcdf_writer.create_netcdf(filename)
    nco.history = (history.decode('utf-8').encode('ascii','replace'))

    coords=output.coords
    cnames=()
    for x in coords:
        netcdf_writer.create_coordinate(nco, x, coords[x].values, coords[x].units)
        cnames=cnames+(x,)
    _crs=output.crs
    if isinstance(_crs, xr.DataArray):
        _crs=CRS(str(_crs.spatial_ref))
    netcdf_writer.create_grid_mapping_variable(nco, _crs)
    for band in output.data_vars:
        output.data_vars[band].values[np.isnan(output.data_vars[band].values)]=nodata
        var= netcdf_writer.create_variable(nco, band, Variable(output.data_vars[band].dtype, nodata, cnames, None) ,set_crs=True)
        var[:] = netcdf_writer.netcdfy_data(output.data_vars[band].values)
    nco.close()
def readNetCDF(file):
    _xarr=xr.open_dataset(file)
    print(_xarr.crs)
    return _xarr
def getUpstreamVariable(task, context,key='return_value'):
    task_instance = context['task_instance']
    upstream_tasks = task.get_flat_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key=key)
    return list(itertools.chain.from_iterable(upstream_variable_values))