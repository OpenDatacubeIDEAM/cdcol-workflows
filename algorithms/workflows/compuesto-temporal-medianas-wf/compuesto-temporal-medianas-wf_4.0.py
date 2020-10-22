#!/usr/bin/python3
# coding=utf8
import xarray as xr
import numpy as np
print ("Compuesto temporal de medianas para " + product['name'])
print(xarr0)
nodata=-9999
medians = {}
time_axis = list(xarr0.coords.keys()).index('time')

print(' lectura de xarr0')
print(xarr0)
print(type(xarr0))
print('asignacion dem')

#dem=xarr0["dem"][0].values

print('finalizacion dem')

print('bandas inicio')
print(type(xarr0.data_vars))
print(xarr0.data_vars)

list_bandas=list(xarr0.data_vars)
print('bandas anterior codigo')

banda1=xarr0['hh']
banda2=xarr0['hv']

#CÁLCULO DE 1ER ÍNDICE RDFI
RDFI =(banda1-banda2)/(banda1+banda2)
#CÁLCULO DE 2DO ÍNDICE SPR
s1  = (banda1+banda2)
s2  =(banda1-banda2)
s3 = 2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.cos(banda1-banda2)
s4 =2 * (banda1 **(1/2))*(banda2 **(1/2)) * np.sin(banda1-banda2)
LPR =(s1+s2)/(s1-s2)
SC = 0.5*s1 - 0.5 * s4
OC = 0.5*s1 + 0.5 * s4
CPR =(SC/OC)
m = (((s2**2)+(s3**2)+(s4**2))**(1/2))/s1

for band in list_bandas:
    print('productbands')
    #print(type('product['bands']'))
    #print(product['bands'])
    if band != 'pixel_qa':
        datos = xarr0.data_vars[band].values
        allNan = ~np.isnan(datos)

        # Comentada por Aurelio (No soporta multi unidad)
        #if normalized:
        #    m=np.nanmean(datos.reshape((datos.shape[time_axis],-1)), axis=1)
        #    st=np.nanstd(datos.reshape((datos.shape[time_axis],-1)), axis=1)
        #    datos=np.true_divide((datos-m[:,np.newaxis,np.newaxis]), st[:,np.newaxis,np.newaxis])*np.nanmean(st)+np.nanmean(m)

        if normalized:
            m=np.nanmean(datos.reshape((datos.shape[time_axis],-1)), axis=1)
            st=np.nanstd(datos.reshape((datos.shape[time_axis],-1)), axis=1)

            # Expand m and st according with the data shape
            # number of coords
            coords_num = len(list(xarr0.coords.keys()))
            l = [ x for x in range(coords_num) if x != time_axis]

            m_new = m
            st_new = st
            for axis in l:
                # If axis is 0  it is equivalent to x[np.newaxis,:]
                # If axis is 1  it is equivalent to x[:,np.newaxis]
                # And so on
                m_new = np.expand_dims(m_new,axis=axis)
                st_new = np.expand_dims(st_new,axis=axis)

            print('Time axis',time_axis)
            print('New axis',l)
            print('m',m.shape)
            print('st',st.shape)
            print('st_new',st_new.shape)
            print('m_new',m_new.shape)
            datos=np.true_divide((datos-m_new), st_new)*np.nanmean(st)+np.nanmean(m)

        medians[band] = np.nanmedian(datos, time_axis)
        medians[band][np.sum(allNan, time_axis) < minValid] = -9999

medians['hh']=banda1[0].values
medians['hv']=banda2[0].values
medians["ndvi"]=np.true_divide(medians["nir"]-medians["red"],medians["nir"]+medians["red"])
medians["ndmi"]=np.true_divide(medians["nir"]-medians["swir1"],medians["nir"]+medians["swir1"])
medians["nbr"]=np.true_divide(medians["nir"]-medians["swir1"],medians["nir"]+medians["swir1"])
medians["nbr2"]=np.true_divide(medians["swir1"]-medians["swir2"],medians["swir1"]+medians["swir2"])
medians["savi"]=(medians["nir"]-medians["red"])/(medians["nir"]+medians["red"]+1)*2 
#medians["gndvi"]=np.true_divide(medians["nir"]-medians["green"],medians["nir"]+medians["green"])
#medians["rvi"]=np.true_divide(medians["nir"],medians["red"])
#medians["nirv"]=(medians["ndvi"] * medians["nir"])
#medians["osavi"]=np.true_divide(medians["nir"]-medians["red"],medians["nir"]+medians["red"]+0.16)
medians['rdfi']=RDFI[0].values
medians['cpR']=CPR[0].values

print('medians_calculated')
del datos

# > **Asignación de coordenadas**
ncoords=[]
xdims =[]
xcords={}
for x in xarr0.coords:
    if(x!='time'):
        ncoords.append( ( x, xarr0.coords[x]) )
        xdims.append(x)
        xcords[x]=xarr0.coords[x]
variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords) for k, v in medians.items()}
output=xr.Dataset(variables, attrs={'crs':xarr0.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=xarr0.coords[x].units

