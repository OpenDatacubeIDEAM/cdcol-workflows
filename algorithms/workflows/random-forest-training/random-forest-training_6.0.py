import os,posixpath
import re
import xarray as xr
import numpy as np
import gdal
import zipfile

import datacube

import pandas as pd

from shapely.geometry import Polygon, MultiPolygon
import json
from datacube.utils import geometry
from datacube.utils.geometry import CRS


from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_validate
from sklearn.metrics import r2_score

from joblib import dump
import rasterio.features

#parametros:
#xarr0: Mosaico del compuesto de medianas
#bands: Las bandas a utilizar
#train_data_path: Ubicación de los shape files .shp

time_ranges = [("2017-01-01", "2017-12-31")] #Una lista de tuplas, cada tupla representa un periodo **** mirar de donde viene


'''
	Code edited by Crhistian Segura
 	Date: 11-Jan-2021
	Modif: Created for biomass algorithm
'''

import pandas as pd


def geometry_mask(geoms, geobox, all_touched=False, invert=False):
    """
    Create a mask from shapes.
    By default, mask is intended for use as a
    numpy mask, where pixels that overlap shapes are False.
    :param list[Geometry] geoms: geometries to be rasterized
    :param datacube.utils.GeoBox geobox:
    :param bool all_touched: If True, all pixels touched by geometries will be burned in. If
                             false, only pixels whose center is within the polygon or that
                             are selected by Bresenham's line algorithm will be burned in.
    :param bool invert: If True, mask will be True for pixels that overlap shapes.
    """
    return rasterio.features.geometry_mask([geom.to_crs(geobox.crs) for geom in geoms],
            out_shape=geobox.shape,
            transform=geobox.affine,
            all_touched=all_touched,
            invert=invert)

# Load csv files
train_zip_file_name  = train_data_path

Conglomerados = pd.read_csv(train_data_path+'/conglomerados.csv',delimiter=',')
#data_set_PPM = pd.read_csv('data_set.csv',delimiter=',')

REFdata = Conglomerados
REFdata['random1'] = np.random.randint(1,11,Conglomerados.shape[0])


## local variables
# Number of folds (k) to run (Kmax should be equal tot he number of k-folds of the training dataset, but you can run less kfolds for debugging purposes)
Kmax = 10

# Point at the attributes names of your shapefile
DV = 'Cha_HD' #Dependent variable name (column with you variable to predict)
#Variable (column) with the folds (if k=10, then the values should be from 1 to 10). Two options
#- 'kfold'-> kfold will be randomly sampled 
#- 'k10' -> kfold will use a prepared stratified random sampling (shapefile attribute) 

kfold = 'k10'

#Variable that count the number of subplot within the cluster
countCluster = 'Count'  
Nplots = 5; #Number of subplots

#Set a maximum value for your dependent variable
maxValueplots = 500

train_kfold = REFdata[REFdata['1ha']==1]
train_kfold = train_kfold[train_kfold[countCluster]>= Nplots]
train_kfold = train_kfold[train_kfold[DV] < maxValueplots]
print(f'kfold dataset: {train_kfold.shape}');


## Generacion de datos de entrenamiento a partir de los conglomerados
salidas=[]
training_labels_all=[]
training_samples_all=np.array([], dtype=np.int64).reshape(0,7)
for i in range(len(train_kfold.Cha_HD)):
    print(f'Running conglom {i+1}')
    try:
        a = json.loads(train_kfold.iloc[i]['.geo'])

        geom = geometry.Geometry(a,crs=CRS('EPSG:4326'))

        dc = datacube.Datacube(app="Cana")
        """
        ALOS = dc.load(
            product='ALOS2_PALSAR_MOSAIC',
            geopolygon=geom,
        )

        ALOS=ALOS.isel(time=0)
        ALOS
        """
        #for i in range(30):
        xarr_0 = dc.load(
            product=product['name'],
            time=time_ranges[0],
            geopolygon=geom,
            measurements=product['bands'],
        )

        mask = geometry_mask([geom], xarr_0.geobox, invert=True)
        
        xarr_0 = xarr_0.where(mask)

        # mascara de nubes
        import numpy as np

        nodata=-9999
        validValues=set()
        if product['name']=="LS7_ETM_LEDAPS" or product['name'] == "LS5_TM_LEDAPS":
            validValues=[66,68,130,132]
        elif product['name'] == "LS8_OLI_LASRC":
            validValues=[322, 386, 834, 898, 1346, 324, 388, 836, 900, 1348]
        else:
            raise Exception("Este algoritmo solo puede enmascarar LS7_ETM_LEDAPS, LS5_TM_LEDAPS o LS8_OLI_LASRC")

        cloud_mask = np.isin(xarr_0["pixel_qa"].values, validValues)
        for band in product['bands']:
            xarr_0[band].values = np.where(np.logical_and(xarr_0.data_vars[band] != nodata, cloud_mask), xarr_0.data_vars[band], np.nan)
        xarr_mask = xarr_0

        # comppuesto de medianas
        normalized = True
        minValid = 1

        medians={} 
        for band in product['bands']:
             if band != 'pixel_qa':
                datos=xarr_mask[band].values
                allNan=~np.isnan(datos) #Una mascara que indica qué datos son o no nan. 
                if normalized: #Normalizar, si es necesario.
                    #Para cada momento en el tiempo obtener el promedio y la desviación estándar de los valores de reflectancia
                    m=np.nanmean(datos.reshape((datos.shape[0],-1)), axis=1)
                    st=np.nanstd(datos.reshape((datos.shape[0],-1)), axis=1)
                    # usar ((x-x̄)/st) para llevar la distribución a media 0 y desviación estándar 1,
                    # y luego hacer un cambio de espacio para la nueva desviación y media. 
                    datos=np.true_divide((datos-m[:,np.newaxis,np.newaxis]), st[:,np.newaxis,np.newaxis])*np.nanmean(st)+np.nanmean(m)
                #Calcular la mediana en la dimensión de tiempo 
                medians[band]=np.nanmedian(datos,0) 
                #Eliminar los valores que no cumplen con el número mínimo de pixeles válidos dado. 
                medians[band][np.sum(allNan,0)<minValid]=np.nan

        del datos
        """
        # normalizar bandas
        medians["red"]   = medians["red"]  /10000
        medians["nir"]   = medians["nir"]  /10000
        medians["swir1"] = medians["swir1"]/10000
        medians["swir2"] = medians["swir2"]/10000
        """
        # Calculo de indices
        medians["ndvi"]=(medians["nir"]-medians["red"])/(medians["nir"]+medians["red"])
        medians["nbr"] =(medians["nir"]-medians["swir2"])/(medians["nir"]+medians["swir2"])
        #medians["nbr2"]=(medians["swir1"]-medians["swir2"])/(medians["swir1"]+medians["swir2"])
        #medians["ndmi"]=(medians["nir"]-medians["swir1"])/(medians["nir"]+medians["swir1"])
        medians["savi"]=((medians["nir"]-medians["red"])/(medians["nir"]+medians["red"] + (0.5)))*(1.5)

        # **Asignacion de coordenadas**
        ncoords=[]
        xdims =[]
        xcords={}
        for x in xarr_0.coords:
            if(x!='time'):
                ncoords.append( ( x, xarr_0.coords[x]) )
                xdims.append(x)
                xcords[x]=xarr_0.coords[x]
        variables ={k: xr.DataArray(v, dims=xdims,coords=ncoords) for k, v in medians.items()}
        output0=xr.Dataset(variables, attrs={'crs':xarr_0.crs})
        for x in output0.coords:
            output0.coords[x].attrs["units"]=xarr_0.coords[x].units
        output0
        """
        # Calbration factor
        # Convertir a .astype(np.int32) para evitar truncamiento
        ALOS32 = ALOS.astype(np.int32)
        # https://www.eorc.jaxa.jp/ALOS-2/en/calval/calval_index.htm
        ALOS_cal = (np.log10(ALOS32*ALOS32)*10)-83
        ALOS_2 = 10**(0.1*ALOS_cal)
        rfid = (ALOS_2.hh-ALOS_2.hv)/(ALOS_2.hh+ALOS_2.hv)
        cpR = ALOS_2.hv/ALOS_2.hh

        ALOS_2
        

        #output0['hh'] = ALOS_2.hh
        #output0['hv'] = ALOS_2.hv
        #output0['rfid']=rfid
        #output0['cpR'] =cpR
        """
        
        output0=output0.where(mask)
        
        labeled_pixels=mask*1
                
        is_train = np.nonzero(labeled_pixels)
        training_labels = labeled_pixels[is_train]
        

        bands_data=[]
        for band in output0.data_vars.keys():
            # pixel_qa is removed from xarr0 by Compuesto Temporal de Medianas
            if band != 'pixel_qa':
                bands_data.append(output0[band])
        bands_data = np.dstack(bands_data)

        # limpiar bands_data[is_train] de nan
        
        training_samples_all = np.concatenate((training_samples_all, bands_data[is_train]))
        training_labels_all= np.concatenate((training_labels_all,training_labels*train_kfold.iloc[i].Cha_HD))
        
        salidas.append(output0)
        
    except Exception as e:
        print('Failed: '+ str(e))
        

print(f'training_samples: {training_samples_all.shape}')
print(f'training_labels: {training_labels_all.shape}')

training_pd_samples=pd.DataFrame(training_samples_all)
training_pd_labels=pd.DataFrame(training_labels_all, columns={DV})

nullData=training_pd_samples[0].isnull()

all_data=pd.concat((training_pd_samples[~nullData],training_pd_labels[~nullData]),axis=1)

all_data_shuf=all_data.sample(frac=1).reset_index(drop=True)

# Select data for clasification
X=all_data_shuf.loc[:,all_data_shuf.columns!=DV]
y=all_data_shuf[DV].to_numpy()

print(f'X shape:{X.shape}')
print(f'y shape:{y.shape}')


# Estimador con los mismos parametros de gee
clf = RandomForestRegressor(n_estimators = 500,
                            max_features = 'sqrt',
                            min_samples_leaf = 1,
                            #bootstrap = True,
                            #max_samples=0.5,
                            max_leaf_nodes = None,
                            random_state = 123)

# K-Fold para la generacion de 10 modelos de RF ( entrenamiento )
cv_results = cross_validate(clf, X, y, cv = 10,
                            scoring = ('r2'),
                            return_estimator=True,
                            return_train_score=True)

# imprim los r2 de los 10 modelos con la data de entrenamiento
for i in range(10):
    ya = cv_results['estimator'][i].predict(X)
    print(f'modelo {i} r2: {r2_score(y, ya):0.4f}')

# Export models to folder
import os

if not os.path.exists(posixpath.join(folder,'models')):
    os.makedirs(posixpath.join(folder,'models'))

from joblib import dump, load
for i in range(10):
    dump(cv_results['estimator'][i], posixpath.join(folder,'models/modelo_'+str(i)+'.joblib'))




"""
# The trainning data must be in a zip folder.
train_zip_file_name  = [file_name for file_name in os.listdir(train_data_path) if file_name.endswith('.zip')][0]
train_zip_file_path = os.path.join(train_data_path,train_zip_file_name)
train_folder_path = train_zip_file_path.replace('.zip','')

print('train_zip_file_path',train_zip_file_path)
print('train_folder_path',train_folder_path)

zip_file = zipfile.ZipFile(train_zip_file_path)
zip_file.extractall(train_data_path)
zip_file.close()

#files = [f for f in os.listdir(train_data_path) if f.endswith('.shp')]
files = [f for f in os.listdir(train_folder_path) if f.endswith('.shp')]
classes = [f.split('.')[0] for f in files]
print(classes)
#shapefiles = [os.path.join(train_data_path, f) for f in files if f.endswith('.shp')]
shapefiles = [os.path.join(train_folder_path, f) for f in files if f.endswith('.shp')]

print('shapefile',shapefiles)

labeled_pixels = rasterizar_entrenamiento(shapefiles, rows, cols, geo_transform, proj)


classifier = RandomForestClassifier(n_jobs=-1, n_estimators=500)

print('trainning samples',X_train)
print('trainning labels',y_train)
#classifier = RandomForestClassifier(n_jobs=-1, n_estimators=50, verbose=1)
classifier.fit(X_train, y_train)

# Calculo de y_pred
print('Estimar y con datos de entrada')
y_pred = classifier.predict(X_test)

# Calculo de matrix de confusion
from sklearn.metrics import confusion_matrix, cohen_kappa_score, precision_score

mconf = confusion_matrix(y_test,y_pred)
# Calculo de kappa score
kappa = cohen_kappa_score(y_test, y_pred)
# Calculo de precision score
prec = precision_score(y_test, y_pred,average = 'weighted')

# Save metrics to file
with open(posixpath.join(folder+'metrics.txt'),'w') as file_metrics:
    print(f'matriz de confusion: {mconf}')
    print(f'kappa score: {kappa}')
    print(f'precision score (weighted): {prec}')
    file_metrics.write('matriz de confusion: \n'+str(mconf))
    file_metrics.write('\nkappa score: '+str(kappa))
    file_metrics.write('\nprecision score (weighted): '+str(prec))

# write shapefiles list
file = open(folder+"shapefiles_list.txt", "w")
file.write("shapefiles list = " + "\n".join(shapefiles))
file.close()



outputxcom=posixpath.join(folder,'modelo_random_forest.pkl')
with open(outputxcom, 'wb') as fid:
    print('output',classifier)
    joblib.dump(classifier, fid)

print(classifier)
"""