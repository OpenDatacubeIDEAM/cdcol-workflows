import xarray as xr
import numpy as np

nodata=-9999


ndvi_periodo_1=[xarrs[k] for k in xarrs.keys() if 'baseline' in k][0];
ndvi_periodo_2=[xarrs[k] for k in xarrs.keys() if 'analysis' in k][0];


print("ndvi_periodo_2")
print(ndvi_periodo_2)

percentage_change = abs((ndvi_periodo_1 - ndvi_periodo_2) / ndvi_periodo_1)

print("percentage_change")
print(percentage_change.max())


anomalies = percentage_change.where(percentage_change>threshold)
print("anomalies")
print(anomalies.max())

output = xr.Dataset(anomalies , attrs={'crs': ndvi_periodo_1.crs})

print("output ")
print(output.max())

#ncoords=[]
#xdims =[]
#xcords={}
#for x in ndvi_periodo_1.coords:
#    if(x!='time'):
#        ncoords.append( ( x, ndvi_periodo_1.coords[x]) )
#        xdims.append(x)
#        xcords[x]=ndvi_periodo_1.coords[x]
#variables ={"ndvi": xr.DataArray(anomalies , dims=xdims,coords=ncoords)}
#output=xr.Dataset(variables, attrs={'crs':ndvi_periodo_1.crs})
#for x in output.coords:
#    output.coords[x].attrs["units"]=ndvi_periodo_1.coords[x].units
