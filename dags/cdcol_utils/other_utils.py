#!/usr/bin/python3
# coding=utf8
from cdcol_plugin.operators import common
import os
import shutil
import glob, zipfile
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
        # if os.path.exists(folder) and len(os.listdir(folder)) == 0:
        #     shutil.rmtree(folder, ignore_errors=True)

def compress_results(execID,**kwargs):
    dag_results_folder = "{}/{}/".format(common.RESULTS_FOLDER, execID)
    if os.path.exists(dag_results_folder) and os.path.isdir(dag_results_folder) and len(os.listdir(dag_results_folder))>0:
            for folder, subfolders, files in os.walk(dag_results_folder):
                print(folder)
                print(subfolders)
                print(files)
                with zipfile.ZipFile(os.path.join(dag_results_folder, "resultados_{}.zip".format(execID)),"w") as file_to_compress:
                    for subfolder in subfolders:
                        if len(os.listdir(os.path.join(folder,subfolder)))==0:
                            shutil.rmtree(os.path.join(folder,subfolder), ignore_errors=True)
                        else:
                            for file in os.listdir(os.path.join(folder,subfolder)):
                                file_to_compress.write(os.path.join(folder, file),os.path.relpath(os.path.join(subfolder, file), dag_results_folder),compress_type=zipfile.ZIP_DEFLATED)
                file_to_compress.close()
                return os.path.join(dag_results_folder, "resultados_{}.zip".format(execID))



