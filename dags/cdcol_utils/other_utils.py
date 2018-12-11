from cdcol_plugin.operators import common
import os
import shutil
def delete_partial_results(algorithms, execID, **kwargs):
    for alg,ver in algorithms.items():
        folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID, alg,ver)
        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder, ignore_errors=True)