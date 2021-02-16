import os,posixpath
import re
import xarray as xr
import numpy as np
import gdal
import zipfile
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib

#parametros:
#xarr0: Mosaico del compuesto de medianas
#bands: Las bandas a utilizar
#train_data_path: Ubicación de los shape files .shp

'''
	Code edited by Crhistian Segura
 	Date: 17-nov-2020
	Modif: add train test split and statistics for validation
'''

def enmascarar_entrenamiento(vector_data_path, cols, rows, geo_transform, projection, target_value=1):
    data_source = gdal.OpenEx(vector_data_path, gdal.OF_VECTOR)
    layer = data_source.GetLayer(0)
    driver = gdal.GetDriverByName('MEM')
    target_ds = driver.Create('', cols, rows, 1, gdal.GDT_UInt16)
    target_ds.SetGeoTransform(geo_transform)
    target_ds.SetProjection(projection)
    gdal.RasterizeLayer(target_ds, [1], layer, burn_values=[target_value])
    return target_ds

def rasterizar_entrenamiento(file_paths, rows, cols, geo_transform, projection):
    labeled_pixels = np.zeros((rows, cols))
    for i, path in enumerate(file_paths):
        label = i+1
        print  ("label")
        print (label)
        ds = enmascarar_entrenamiento(path, cols, rows, geo_transform, projection, target_value=label)
        band = ds.GetRasterBand(1)
        labeled_pixels += band.ReadAsArray()
        print  ("labeled_pixels")
        print (labeled_pixels)
        #ds = None
    return labeled_pixels

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

rows, cols = xarr0[product['bands'][0]].shape
_coords=xarr0.coords


#(originX, pixelWidth, 0, originY, 0, pixelHeight)
#geo_transform=(_coords["longitude"].values[0], 0.000269995,0, _coords["latitude"].values[0],0,-0.000271302)
#proj = xarr0.crs.crs_wkt
# Modified by crhistian to fix _coords order
geo_transform=(float(_coords["longitude"].min()), 0.000269995,0, float(_coords["latitude"].max()),0,-0.000271302)
proj = xarr0.crs.crs_wkt


print('shapefile',shapefiles)

labeled_pixels = rasterizar_entrenamiento(shapefiles, rows, cols, geo_transform, proj)

is_train = np.nonzero(labeled_pixels)
training_labels = labeled_pixels[is_train]
bands_data=[]

print(xarr0)

for band in product['bands']:
    # pixel_qa is removed from xarr0 by Compuesto Temporal de Medianas
    if band != 'pixel_qa':
        bands_data.append(xarr0[band])
bands_data = np.dstack(bands_data)
training_samples = bands_data[is_train]
print('training_samples')
#print(training_samples.shape())

rows, cols, n_bands = bands_data.shape

np.isfinite(training_samples)
_msk=np.sum(np.isfinite(training_samples),1)>1
training_samples= training_samples[_msk,:]
training_labels=training_labels[_msk]

#mascara valores nan por valor no data
mask_nan=np.isnan(training_samples)
training_samples[mask_nan]=-9999
print('training_samples')
print(training_samples)
#print(training_samples.shape())

print('training_labels')
print(training_labels)
#print(training_labels.shape())

# Split train/test
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(training_samples, training_labels, test_size=0.3)

print(f'Train X {len(X_train)/len(training_samples)*100:.2f}%')
print(f'Train Y {len(y_train)/len(training_samples)*100:.2f}%')
print(f'Test  X {len(X_test )/len(training_samples)*100:.2f}%')
print(f'Test  Y {len(y_test )/len(training_samples)*100:.2f}%')

#import matplotlib.pyplot as plt
#plt.hist(y_train,bins=np.arange(1,len(np.unique(training_labels))+2)-0.5)
#plt.rcParams['figure.facecolor'] = 'white'
#plt.savefig(posixpath.join(folder,'Histogram_tain_data.png'))
print('Se usaran 500 arboles!!')
classifier = RandomForestClassifier(n_jobs=-1, n_estimators=500, verbose=1)

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
