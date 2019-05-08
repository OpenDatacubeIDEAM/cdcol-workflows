#!/usr/bin/python3
# coding=utf8
from cdcol_plugin.operators import common
import os
import shutil
import glob
def delete_all_partial_results(algorithms, execID, **kwargs):
    for alg,ver in algorithms.items():
        folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID, alg,ver)
        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder, ignore_errors=True)

def delete_partial_result(algorithm, version, execID, task_id, **kwargs):
    folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID, algorithm, version)
    if os.path.exists(folder) and os.path.isdir(folder):
        files=glob.glob("{}*{}*".format(folder,task_id))
        print(files)
        for f in files:
            os.remove(f)

        if os.path.exists(folder) and len(os.listdir(folder)) == 0:
            shutil.rmtree(folder, ignore_errors=True)