import numpy as np

ndvi_baseline = [xarrs[k] for k in xarrs.keys() if 'baseline' in k][0];
ndvi_analysis =[xarrs[k] for k in xarrs.keys() if 'analysis' in k][0];

ndvi_baseline.values[scene_cleaned.red.values == -9999] = -9999
ndvi_baseline.values[baseline_mosaic.red.values == -9999] = -9999
ndvi_scene.values[scene_cleaned.red.values == -9999] = -9999
ndvi_scene.values[baseline_mosaic.red.values == -9999] = -9999

percentage_change = abs((ndvi_baseline - ndvi_scene) / ndvi_baseline)

anomalies = percentage_change.where(percentage_change > threshold_sel.value)

#anomalies = percentage_change.where(percentage_change > threshold_sel.value)


print(ndvi_baseline)
print(ndvi_analysis)
print(percentage_change)
#_min, _max = ndvi_baseline_threshold_range
#baseline_ndvi_filter_mask = np.logical_and(NDVI(baseline_composite) > _min, NDVI(baseline_composite) < _max)
