#!/usr/bin/python3
# coding=utf8

#from datacube.storage import netcdf_writer
#from datacube.model import Variable
import datacube
import numpy as np
from datacube.drivers.netcdf import writer as netcdf_writer
from datacube.utils.geometry import CRS
from datacube.utils import geometry, data_resolution_and_offset
from rasterio.transform import from_bounds
from rasterio.warp import reproject, Resampling
from rasterio.coords import BoundingBox
from subprocess import CalledProcessError, Popen, PIPE, check_output
from affine import Affine
import rasterio
import os, glob
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
    geobox=calculate_bounds_geotransform(dataset)
    # geotransform = from_bounds(dataset.longitude[0], dataset.latitude[-1], dataset.longitude[-1], dataset.latitude[0],
    #                            len(dataset.longitude), len(dataset.latitude))
    geotransform = from_bounds(geobox['left'], geobox['top'], geobox['right'], geobox['bottom'],
                               len(dataset.longitude), len(dataset.latitude))
    print(geotransform)
    return geotransform

def calculate_bounds_geotransform(dataset):
    crs_dict = dataset.crs.to_dict()
    _crs = CRS(str(crs_dict['attrs']['spatial_ref']))
    dims = _crs.dimensions
    print(dims)
    xres, xoff = data_resolution_and_offset(dataset[dims[1]])
    yres, yoff = data_resolution_and_offset(dataset[dims[0]])
    GeoTransform = [xoff, xres, 0.0, yoff, 0.0, yres]
    left, right = dataset[dims[1]][0] - 0.5 * xres, dataset[dims[1]][-1] + 0.5 * xres
    bottom, top = dataset[dims[0]][0] - 0.5 * yres, dataset[dims[0]][-1] + 0.5 * yres
    return {'left':left, 'right':right,'bottom':bottom, 'top':top, 'GeoTransform': GeoTransform}



def write_geotiff_from_xr(tif_path, dataset, bands=[], no_data=-9999, crs="EPSG:4326"):

    """Write a geotiff from an xarray dataset.

    Args:
        tif_path: path for the tif to be written to.
        dataset: xarray dataset
        bands: list of strings representing the bands in the order they should be written
        no_data: nodata value for the dataset
        crs: requested crs.
        Affine(a,b,c,d,e,f)
        a = width of a pixel
        b = row rotation (typically zero)
        c = x-coordinate of the upper-left corner of the upper-left pixel
        d = column rotation (typically zero)
        e = height of a pixel (typically negative)
        f = y-coordinate of the of the upper-left corner of the upper-left pixel

    """
    from rasterio.crs import CRS as CRS_rasterio
    bands=list(dataset.data_vars.keys())
    assert isinstance(bands, list), "Bands must a list of strings"
    assert len(bands) > 0 and isinstance(bands[0], str), "You must supply at least one band."
    if dataset.crs is not None:
        if isinstance(dataset.crs, xr.DataArray):
            crs_dict = dataset.crs.to_dict()
            crs = CRS_rasterio.from_wkt(crs_dict['attrs']['crs_wkt'])
            geobox = calculate_bounds_geotransform(dataset)
            bounds = BoundingBox(left=geobox['left'], bottom=geobox['bottom'], right=geobox['right'], top=geobox['top'])
        else:
            crs = dataset.crs.crs_str
    else:
        transform = _get_transform_from_xr(dataset)

    transform = _get_transform_from_xr(dataset)

    with rasterio.open(tif_path,'w',
                       driver='GTiff',
                       height=dataset.dims['latitude'],
                       width=dataset.dims['longitude'],
                       count=len(bands),
                       dtype=dataset[bands[0]].dtype,#str(dataset[bands[0]].dtype),
                       crs=crs,
                       transform=transform,
                       bounds=bounds,
                       nodata=no_data) as dst:
        for index, band in enumerate(bands):
            print(dataset[band].dtype)
            dst.write_band(index + 1, dataset[band].values.astype(dataset[bands[0]].dtype), )
            tag = {'Band_'+str(index+1): bands[index]}
            dst.update_tags(**tag)
            dst.set_band_description(index + 1, band)
        dst.close()


def translate_netcdf_to_tiff(task_id, algorithm,folder,files):
    bash_script_path = os.path.join(ALGORITHMS_FOLDER, "generate-geotiff", "generate-geotiff_1.0.sh")
    try:
        p = Popen([bash_script_path, task_id, algorithm, folder] + files, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        stdout = stdout.decode('utf-8')
        stderr = stderr.decode('utf-8')
        # out = check_output([bash_script_path, folder]+_files)
        if stdout:
            print(stdout)
            return glob.glob("{}*{}*".format(folder, task_id))
        else:
            print(stderr)
            raise AirflowSkipException("ERROR")

    except CalledProcessError as cpe:
        print('Error: No se pudo generarel geotiff ')
