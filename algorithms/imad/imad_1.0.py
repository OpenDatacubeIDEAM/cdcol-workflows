#consulta de mosaico
inDataset1=[]
#mosaico de LS8
inDataset2=[]

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

# Calculo de imagen de cambios entre la imagen de referencia y la imagen objetivo, seleccionando el mejor delta de cambio.
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