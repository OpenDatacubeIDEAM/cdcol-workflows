import xarray as xr
import numpy as np

ndvi_periodo_1 = [xarrs[k] for k in xarrs.keys() if 'baseline' in k][0];
ndvi_periodo_2 =[xarrs[k] for k in xarrs.keys() if 'analysis' in k][0];

ndvi_periodo_1.values[scene_cleaned.red.values == -9999] = -9999
ndvi_periodo_1.values[baseline_mosaic.red.values == -9999] = -9999
ndvi_periodo_2.values[scene_cleaned.red.values == -9999] = -9999
ndvi_periodo_2.values[baseline_mosaic.red.values == -9999] = -9999

percentage_change = abs((ndvi_periodo_2 - ndvi_periodo_1) / ndvi_periodo_2)

anomalies = percentage_change.where(percentage_change > threshold_sel.value)



print(ndvi_baseline)
print(ndvi_analysis)
print(percentage_change)
#_min, _max = ndvi_baseline_threshold_range
#baseline_ndvi_filter_mask = np.logical_and(NDVI(baseline_composite) > _min, NDVI(baseline_composite) < _max)
