import sys
import numpy as np
import time
from arrnorm.auxil.auxil import similarity
import matplotlib.pyplot as plt
from scipy import stats
from arrnorm.auxil.auxil import orthoregress
from operator import itemgetter
from scipy import linalg, stats
import arrnorm.auxil.auxil as auxil
from datetime import datetime
import xarray as xr



#consulta de mosaico
inDataset2=[xarrs[k] for k in xarrs.keys() if 'consulta_referencia' in k][0];
#mosaico de LS8
inDataset1=[xarrs[k] for k in xarrs.keys() if 'medianas' in k][0];

band_pos=None
dims=None
max_iters=20

try:
    rows = list(inDataset1.dims.values())[0]
    cols = list(inDataset1.dims.values())[1]
    bands = len(inDataset1.data_vars)
    rows2 = list(inDataset2.dims.values())[0]
    cols2 = list(inDataset2.dims.values())[1]
    bands2 = len(inDataset2.data_vars)
except Exception as err:
    print('Error: {}  --Images could not be read.'.format(err))
    sys.exit(1)

if bands != bands2:
    sys.stderr.write("The number of bands does not match between reference and target image")
    sys.exit(1)
if band_pos is None:
    band_pos = list(range(1, bands + 1))
else:
    bands = len(band_pos)
if dims is None:
    x0 = 0
    y0 = 0
else:
    x0, y0, cols, rows = dims

x2 = x0
y2 = y0

print('------------IRMAD -------------')
print(time.asctime())
start = time.time()

# iteration of MAD
cpm = auxil.Cpm(2 * bands)
print(type(cpm))
delta = 1.0
oldrho = np.zeros(bands)
current_iter = 0
tile = np.zeros((cols, 2 * bands))
sigMADs = 0
means1 = 0
means2 = 0
A = 0
B = 0
rasterBands1 = []
rasterBands2 = []
rhos = np.zeros((max_iters, bands))
results = []

for band in Bands:
    rasterBands1.append(inDataset1[band])
    rasterBands2.append(inDataset2[band])

# check if the band data has only zeros
for band in range(len(Bands)):
    # check the reference image
    if not rasterBands1[band].any():
        sys.exit(1)
    # check the target image
    if not rasterBands2[band].any():
        sys.exit(1)

while current_iter < max_iters:
    try:
        for row in range(rows):
            for k in range(len(Bands)):
                bandSour = np.asarray(rasterBands2[k])
                tile[:, k] = np.asarray([bandSour[0][row]])
                bandTarg = np.asarray(rasterBands1[k])
                tile[:, bands + k] = np.asarray([bandTarg[0][row]])
            tile = np.nan_to_num(tile)
            tst1 = np.sum(tile[:, 0:bands], axis=1)
            tst2 = np.sum(tile[:, bands::], axis=1)
            idx1 = set(np.where((tst1 != 0))[0])
            idx2 = set(np.where((tst2 != 0))[0])
            idx = list(idx1.intersection(idx2))
            if current_iter > 0:
                mads = np.asarray((tile[:, 0:bands] - means1) * A - (tile[:, bands::] - means2) * B)
                chisqr = np.sum((mads / sigMADs) ** 2, axis=1)
                wts = 1 - stats.chi2.cdf(chisqr, [bands])
                cpm.update(tile[idx, :], wts[idx])
            else:
                cpm.update(tile[idx, :])
        # weighted covariance matrices and means
        S = cpm.covariance()
        means = cpm.means()
        # reset prov means object
        cpm.__init__(2 * bands)
        s11 = S[0:bands, 0:bands]
        s22 = S[bands:, bands:]
        s12 = S[0:bands, bands:]
        s21 = S[bands:, 0:bands]
        c1 = s12 * linalg.inv(s22) * s21
        b1 = s11
        c2 = s21 * linalg.inv(s11) * s12
        b2 = s22
        # solution of generalized eigenproblems
        if bands > 1:
            mu2a, A = auxil.geneiv(c1, b1)
            mu2b, B = auxil.geneiv(c2, b2)
            # sort a
            idx = np.argsort(mu2a)
            A = A[:, idx]
            # sort b
            idx = np.argsort(mu2b)
            B = B[:, idx]
            mu2 = mu2b[idx]
        else:
            mu2 = c1 / b1
            A = 1 / np.sqrt(b1)
            B = 1 / np.sqrt(b2)
        # canonical correlations
        rho = np.sqrt(mu2)
        b2 = np.diag(B.T * B)
        sigma = np.sqrt(2 * (1 - rho))
        # stopping criterion
        delta = max(abs(rho - oldrho))
        rhos[current_iter, :] = rho
        oldrho = rho
        # tile the sigmas and means
        sigMADs = np.tile(sigma, (cols, 1))
        means1 = np.tile(means[0:bands], (cols, 1))
        means2 = np.tile(means[bands::], (cols, 1))
        # ensure sum of positive correlations between X and U is positive
        D = np.diag(1 / np.sqrt(np.diag(s11)))
        s = np.ravel(np.sum(D * s11 * A, axis=0))
        A = A * np.diag(s / np.abs(s))
        # ensure positive correlation between each pair of canonical variates
        cov = np.diag(A.T * s12 * B)
        B = B * np.diag(cov / np.abs(cov))
        current_iter += 1

        results.append((delta, {"iter": current_iter, "A": A, "B": B, "means1": means1, "means2": means2,
                                "sigMADs": sigMADs, "rho": rho}))
        del s11, s22
        del s12, s21
        del c1, b1, c2, b2

    except Exception as err:
        print("\n WARNING: Occurred a exception value error for the last iteration No. {0},\n"
              " then the ArrNorm will be use the best result at the moment calculated, you\n"
              " should check the result and all bands in input file if everything is correct.".format(current_iter))
        # ending the iteration
        # current_iter = max_iters
    if current_iter == max_iters:  # end iteration
        # select the result with the best delta
        best_results = sorted(results, key=itemgetter(0))[0]
        print("\n The best delta for all iterations is {0}, iter num: {1},\n"
              " making the final result normalization with this parameters.".
              format(round(best_results[0], 5), best_results[1]["iter"]))

        # set the parameter for the best delta
        delta = best_results[0]
        A = best_results[1]["A"]
        B = best_results[1]["B"]
        means1 = best_results[1]["means1"]
        means2 = best_results[1]["means2"]
        sigMADs = best_results[1]["sigMADs"]
        rho = best_results[1]["rho"]

outBands = []
for band in range(bands + 1):
    outBands.append(np.zeros([rows,cols]))

for row in range(rows):
    for k in range(bands):
        bandSour = np.asarray(rasterBands2[k])
        tile[:, k] = np.asarray([bandSour[0][row]])
        bandTarg = np.asarray(rasterBands1[k])
        tile[:, bands + k] = np.asarray([bandTarg[0][row]])
    mads = np.asarray((tile[:, 0:bands] - means1) * A - (tile[:, bands::] - means2) * B)
    chisqr = np.sum((mads / sigMADs) ** 2, axis=1)
    for k in range(bands):
        outBands[k][row]=np.asarray([mads[:,k]])
    outBands[bands][row]=np.asarray([chisqr])
    del mads,chisqr
    #outBand.FlushCache()

for k in range(len(Bands)):
    bandSour = np.asarray(rasterBands2[k])
    tile[:, k] = np.asarray([bandSour[0][row]])

np.asarray(outBands[bands])

ncpThresh=0.15
pos=None
dims=None
img_target=None
graphics=False

try:
    rows = list(inDataset1.dims.values())[0]
    cols = list(inDataset1.dims.values())[1]
    bands = len(inDataset1.data_vars)

except Exception as err:
    print('Error: {}  --Images could not be read.'.format(err))
    sys.exit(1)

if pos is None:
    pos = list(range(1, bands + 1))
else:
    bands = len(band_pos)
if dims is None:
    x0 = 0
    y0 = 0
else:
    x0, y0, cols, rows = dims

chisqr = np.asarray(outBands[bands]).ravel()
ncp = 1 - stats.chi2.cdf(chisqr, [bands - 1])
idx = where(ncp > ncpThresh)

aa = []
bb = []

j = 1
bands = len(pos)

# Cáculo ortoregresión con imagen referencia e imagen objetivo obteniendo valores de pendiente, intercepción y correlación entre las mismas.

outBand = []
for k in pos:
    x = np.asarray(rasterBands2[k - 1]).ravel()
    y = np.asarray(rasterBands1[k - 1]).ravel()
    b, a, R = orthoregress(y[idx], x[idx])
    print('band: {}  slope: {} intercept: {}  correlation: {}'.format(k, b, a, R))
    my = max(y[idx])
    aa.append(a)
    bb.append(b)
    outBand.append(np.zeros([rows, cols]))
    # Imagen normalizada resultante
    outBand[k - 1] = resize(a + b * y, (rows, cols))
    # outBand.FlushCache()
    j += 1

ncoords=[]
xdims =[]
xcords={}
for x in nbar.coords:
    if(x!='time'):
        ncoords.append( ( x, nbar.coords[x]) )
        xdims.append(x)
        xcords[x]=nbar.coords[x]
test = {}
for k in range(bands):
    test[Bands[k]] = outBand[k]

variables = {k: xr.DataArray(v, dims=xdims, coords=ncoords)
             for k, v in test.items()}
output = xr.Dataset(variables, attrs={'crs': nbar.crs})
for x in output.coords:
    output.coords[x].attrs["units"]=nbar.coords[x].units