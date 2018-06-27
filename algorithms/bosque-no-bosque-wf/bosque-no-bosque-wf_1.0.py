import xarray as xr
import numpy as np
print "Excecuting forest_noforest v1 "

period_nvdi=xarr0["ndvi"].values
height = period_nvdi.shape[0]
width = period_nvdi.shape[1]
bosque_nobosque=np.full(period_nvdi.shape, -1)
for y1 in xrange(0, height, slice_size):
    for x1 in xrange(0, width, slice_size):
        x2 = x1 + slice_size
        y2 = y1 + slice_size
        if(x2 > width):
            x2 = width
        if(y2 > height):
            y2 = height
        submatrix = period_nvdi[y1:y2,x1:x2]
        ok_pixels = np.count_nonzero(~np.isnan(submatrix))
        if ok_pixels==0:
            bosque_nobosque[y1:y2,x1:x2] = -9999    
        elif float(np.nansum(submatrix>ndvi_threshold))/float(ok_pixels) >= vegetation_rate :
            bosque_nobosque[y1:y2,x1:x2] = 1
        else:
            bosque_nobosque[y1:y2,x1:x2] = 0


ncoords=[]
xdims =[]
xcords={}
nbar=xarr0
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"bosque_nobosque": xr.DataArray(bosque_nobosque.astype(np.int16), dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
print output