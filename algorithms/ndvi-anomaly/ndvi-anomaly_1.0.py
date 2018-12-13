import numpy as np

ndvi_baseline = [xarrs[k] for k in xarrs.keys() if 'baseline' in k][0];
ndvi_analysis =[xarrs[k] for k in xarrs.keys() if 'analysis' in k][0];

print(ndvi_baseline)
print(ndvi_analysis)
print(ndvi_baseline_threshold_range)
#_min, _max = ndvi_baseline_threshold_range
#baseline_ndvi_filter_mask = np.logical_and(NDVI(baseline_composite) > _min, NDVI(baseline_composite) < _max)