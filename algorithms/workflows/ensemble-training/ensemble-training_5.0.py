import os,posixpath
import re
import xarray as xr
import numpy as np
import gdal
import zipfile
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import BaggingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.svm import SVC

#parametros:
#xarr0: Mosaico del compuesto de medianas
#bands: Las bandas a utilizar
#train_data_path: Ubicación de los shape files .shp

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


files = [f for f in os.listdir(train_folder_path) if f.endswith('.shp')]
classes = [f.split('.')[0] for f in files]

shapefiles = [os.path.join(train_folder_path, f) for f in files if f.endswith('.shp')]

rows, cols = xarr0[product['bands'][0]].shape

print('rows',rows)
print('cols',cols)

_coords=xarr0.coords

print('bandas xarr0',list(xarr0.data_vars))
lista=list(xarr0.data_vars)

#(originX, pixelWidth, 0, originY, 0, pixelHeight)
geo_transform=(_coords["longitude"].values[0], 0.000269995,0, _coords["latitude"].values[0],0,-0.000271302)
proj = xarr0.crs.crs_wkt

#print('shapefile_docs',shapefiles)

labeled_pixels = rasterizar_entrenamiento(shapefiles, rows, cols, geo_transform, proj)

is_train = np.nonzero(labeled_pixels)
training_labels = labeled_pixels[is_train]

# Preprocesar:
#nmed=None
#nan_mask=None
#xarrs=list(xarr0.values())
#print(type(xarrs))
#medians1 = xarrs[0]

#print("medianas")
#print(type(medians1))
print("medianas",xarr0)
print("fin consulta mediana")
#print('medians1.datavars',medians1.data_vars.keys())


bands_data=[]


#bands2=list(xarr0.data_vars.keys())


for band in lista:
    #print('bands',product['bands'])
    # pixel_qa is removed from xarr0 by Compuesto Temporal de Medianas
    if band != 'pixel_qa':
        bands_data.append(xarr0[band])
bands_data = np.dstack(bands_data)
training_samples = bands_data[is_train]
print('training_samples')
print(training_samples.shape)

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

print('training_labels')
print(training_labels)


print('training_labels')
print(training_labels.shape)

from sklearn.ensemble import ExtraTreesClassifier
#%%time
rf = RandomForestClassifier(n_jobs=-1, n_estimators=500, verbose=1)
dtree=tree.DecisionTreeClassifier(criterion='gini')
svml=SVC(C=1.0,  class_weight='balanced',decision_function_shape='ovr', degree=3, gamma='auto', kernel='linear',
           max_iter=-1, probability=False, random_state=None, shrinking=True,tol=0.001, verbose=False)
knn = KNeighborsClassifier(algorithm='brute',n_neighbors=3,metric='mahalanobis')
nn = MLPClassifier(alpha=0.0001,  hidden_layer_sizes=(500,),random_state=None,max_iter=500,activation = 'logistic',solver='adam')
grad_boost=GradientBoostingClassifier(n_estimators=500,learning_rate=1)
extrat = ExtraTreesClassifier(n_estimators=50, max_depth=None,class_weight='balanced')

clf_array=[rf,extrat,dtree,svml,grad_boost]#svml,nn,grad_boost,extrat,dtree

for clf in clf_array:
    vanilla_scores = cross_val_score(clf, training_samples, training_labels, cv=2, n_jobs=-1)
    bagging_clf = BaggingClassifier(clf)
    bagging_scores = cross_val_score(bagging_clf,training_samples, training_labels, cv=2, 
       n_jobs=-1)

    print ("Mean of: {1:.3f}, std: (+/-) {2:.3f}[{0}]"  
                       .format(clf.__class__.__name__, 
                       vanilla_scores.mean(), vanilla_scores.std()))
    print ("Mean of: {1:.3f}, std: (+/-) {2:.3f} [Bagging {0}]\n"
                       .format(clf.__class__.__name__, 
                        bagging_scores.mean(), bagging_scores.std()))


bagging_clf.fit(training_samples, training_labels)

print('bagging mean')
print(bagging_scores.mean())

print('bagging scores')
print(bagging_scores.mean())

print('bagging clf')
print(bagging_clf)


outputxcom=posixpath.join(folder,'modelo_random_forest_2.pkl')
with open(outputxcom, 'wb') as fid:
    joblib.dump(bagging_clf, fid)



rows, cols, n_bands = bands_data.shape

print('rows',rows)
print('cols',cols)

print('n_bands',n_bands)

print('bagging clf min max')
#print(bagging_clf.min())
#print(bagging_clf.max())

n_samples = rows*cols
flat_pixels = bands_data.reshape((n_samples, n_bands))

print('flat_pixels')
print(type(flat_pixels))
print(flat_pixels.min())
print(flat_pixels.max())
print(flat_pixels)


#mascara valores nan por valor no data
mask_nan=np.isnan(flat_pixels)
flat_pixels[mask_nan]=-9999

flat_pixels[flat_pixels<0]=0
#fnf_min=flat_pixels.values[0]<1
#fnf_min=np.isin(inDataset2["fnf_mask"].values[0]==1, np.nan)

#print(fnf_min)

print('flat_pixels 2')
print(type(flat_pixels))
print(flat_pixels.min())
print(flat_pixels.max())
print(flat_pixels)

from numpy import inf

flat_pixels[flat_pixels==-inf]= 0
flat_pixels[flat_pixels>= 1E308]=0

print('flat_pixels 3')
print(type(flat_pixels))
print(flat_pixels.min())
print(flat_pixels.max())
print(flat_pixels)


#np.isfinite(flat_pixels)

_msk=np.sum(np.isfinite(flat_pixels),1)>1
#flat_pixels= flat_pixels[_msk,:]
flat_pixels=flat_pixels[_msk]


print(flat_pixels.min())
print(flat_pixels.max())
#result = bagging_clf.predict(flat_pixels)
#classification = result.reshape((rows, cols))

print("clasificacion final")
result = bagging_clf.predict(flat_pixels)
result = result.reshape((rows, cols))
print("fin funcion de clasificacion")


coordenadas = []
dimensiones = []
xcords = {}
for coordenada in xarr0[0].coords:
    if (coordenada != 'time'):
        coordenadas.append((coordenada, xarr0[0].coords[coordenada]))
        dimensiones.append(coordenada)
        xcords[coordenada] = xarr0[0].coords[coordenada]

valores = {"classified": xr.DataArray(result, dims=dimensiones, coords=coordenadas)}
#array = xr.DataArray(result, dims=dimensiones, coords=coordenadas)
#array.astype('float32')
#valores = {"classified": array}

output = xr.Dataset(valores, attrs={'crs': xarr0[0].crs})
for coordenada in output.coords:
    output.coords[coordenada].attrs["units"] = xarr0[0].coords[coordenada].units

classified = output.classified
classified.values = classified.values.astype('float32')
