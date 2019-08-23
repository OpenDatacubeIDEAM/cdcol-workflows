import hdmedians as hd
import xarray as xr
import numpy as np
import datacube

dc = datacube.Datacube(app='geomedia', config = "/home/cubo/.datacube.conf")

#extents = {
#    "crs": {
#         "longitude": (-74.026,-74), 
#        "latitude": (3.28,3.3),
#        "product" : "LS8_OLI_LASRC",
               
#    },        
#}

#xarr0 = xarr0.drop('crs')
#xarr0 = dc.load(**extents["crs"])

def nan_to_num(dataset, number):
    for key in list(dataset.data_vars):
        dataset[key].values[np.isnan(dataset[key].values)] = number

def create_hdmedians_multiple_band_mosaic(dataset_in,
					  no_data=-9999,
                                          intermediate_product=None,
                                          operation="median",
                                          **kwargs):

    #assert clean_mask is not None, "A boolean mask for clean_mask must be supplied."
    assert operation in ['median']

    dataset_in_filtered = dataset_in.where(dataset_in != no_data)

    band_list = list(dataset_in_filtered.data_vars)
    arrays = [dataset_in_filtered[band] for band in band_list]

    stacked_data = np.stack(arrays)
    bands_shape, time_slices_shape, lat_shape, lon_shape = stacked_data.shape[0], stacked_data.shape[
        1], stacked_data.shape[2], stacked_data.shape[3]

    reshaped_stack = stacked_data.reshape(bands_shape, time_slices_shape,
                                          lat_shape * lon_shape)  # Reshape to remove lat/lon
    hdmedians_result = np.zeros((bands_shape, lat_shape * lon_shape))  # Build zeroes array across time slices.

    for x in range(reshaped_stack.shape[2]):
        try:
            hdmedians_result[:, x] = hd.nangeomedian(
                reshaped_stack[:, :, x], axis=1)
        except ValueError:
            no_data_pixel_stack = reshaped_stack[:, :, x]
            no_data_pixel_stack[np.isnan(no_data_pixel_stack)] = no_data
            hdmedians_result[:, x] = np.full((bands_shape), no_data)

    output_dict = {
        value: (('latitude', 'longitude'), hdmedians_result[index, :].reshape(lat_shape, lon_shape))
        for index, value in enumerate(band_list)
    }
    dataset_out = xr.Dataset(output_dict,
                             coords={'latitude': dataset_in['latitude'], 'longitude': dataset_in['longitude']},
                             attrs = dataset_in.attrs)
    nan_to_num(dataset_out, no_data)
    return dataset_out

#crs_org = xarr0.crs
output = create_hdmedians_multiple_band_mosaic(xarr0)
del xarr0
