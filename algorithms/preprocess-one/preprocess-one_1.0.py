nmed=None
nan_mask=None
for band in medians1:
    b=medians1[band].ravel()
    if nan_mask is None:
        nan_mask=np.isnan(b)
    else:
        nan_mask=np.logical_or(nan_mask, np.isnan(medians1[band].ravel()))
    b[np.isnan(b)]=np.nanmedian(b)
    if nmed is None:
        sp=medians1[band].shape
        nmed=b
    else:
        nmed=np.vstack((nmed,b))
del medians1
