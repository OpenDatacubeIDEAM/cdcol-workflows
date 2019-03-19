import time
import numpy as np
import rasterio

def _get_transform_from_xr(dataset):
    """Create a geotransform from an xarray dataset.
    """

    from rasterio.transform import from_bounds
    geotransform = from_bounds(dataset.longitude[0], dataset.latitude[-1], dataset.longitude[-1], dataset.latitude[0],
                               len(dataset.longitude), len(dataset.latitude))
    return geotransform

def write_geotiff_from_xr(tif_path, dataset, bands, no_data=-9999, crs="EPSG:4326"):
    """Write a geotiff from an xarray dataset.

    Args:
        tif_path: path for the tif to be written to.
        dataset: xarray dataset
        bands: list of strings representing the bands in the order they should be written
        no_data: nodata value for the dataset
        crs: requested crs.

    """
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
            dst.write(dataset[band].values, index + 1)
        dst.close()
		
def export_slice_to_geotiff(ds, path):
    """
    Exports a single slice of an xarray.Dataset as a GeoTIFF.
    
    ds: xarray.Dataset
        The Dataset to export.
    path: str
        The path to store the exported GeoTIFF.
    """
    kwargs = dict(tif_path=path, dataset=ds.astype(np.float32), bands=list(ds.data_vars.keys()))
    if 'crs' in ds.attrs:
        kwargs['crs'] = str(ds.attrs['crs'])
    write_geotiff_from_xr(**kwargs)

def export_xarray_to_geotiff(ds, path):
    """
    Exports an xarray.Dataset as individual time slices.
    
    Parameters
    ----------
    ds: xarray.Dataset
        The Dataset to export.
    path: str
        The path prefix to store the exported GeoTIFFs. For example, 'geotiffs/mydata' would result in files named like
        'mydata_2016_12_05_12_31_36.tif' within the 'geotiffs' folder.
    """
    def time_to_string(t):
        return time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime(t.astype(int)/1000000000))
    
    for t in ds.time:
        time_slice_xarray = ds.sel(time = t)
        export_slice_to_geotiff(time_slice_xarray,
                                path + "_" + time_to_string(t) + ".tif")

## End export ##