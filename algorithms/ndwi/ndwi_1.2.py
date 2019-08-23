import xarray as xr
import numpy as np
print "Excecuting ndwi v1 "


nbar = xarr0
period_green = nbar["green"].values	
period_nir = nbar["nir"].values	
mask_nan=np.logical_or(np.isnan(period_green), np.isnan(period_nir))
period_ndwi = np.true_divide( np.subtract(period_green,period_nir) , np.add(period_green,period_nir) )
period_ndwi[mask_nan]=np.nan
#Hace un clip para evitar valores extremos. 
period_ndwi[period_ndwi>1]=np.nan
period_ndwi[period_ndwi<-1]=np.nan
import xarray as xr
ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
variables ={"ndwi": xr.DataArray(period_ndwi, dims=xdims,coords=ncoords)}
output=xr.Dataset(variables, attrs={'crs':nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units
