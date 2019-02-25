#!/usr/bin/python3
# coding=utf8

import os, errno
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from cdcol_plugin.operators import common
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
from subprocess import CalledProcessError, Popen, PIPE, check_output

class CDColBashOperator(BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, execID, algorithm, version, product=None,str_files=None, lat=None, lon=None, output_type="output",params={}, *args,**kwargs):
        super(CDColBashOperator,self).__init__(*args,**kwargs)
        self.execID = execID
        self.algorithm = algorithm
        self.version = version
        self.str_files = str_files
        self.lat = lat
        self.lon = lon
        self.alg_kwargs = params
        self.folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID, algorithm, version, )
        self.product = product
        self.output_type = output_type

    def execute(self, context):
        if not os.path.exists(os.path.dirname(self.folder)):
            try:
                os.makedirs(os.path.dirname(self.folder))
            except OSError as exc:
                if exc.errno != errno.EXIST:
                    raise
        folder=self.folder
        if self.str_files is None:
            self.str_files=common.getUpstreamVariable(self, context)
        if self.str_files is None or len(self.str_files) == 0:
            raise AirflowSkipException("ERROR: No hay archivos para procesar del anterior paso")
        _files = [x for x in self.str_files if self.output_type in x]
        bash_script_path=common.ALGORITHMS_FOLDER+"/"+self.algorithm+"/"+self.algorithm+"_"+str(self.version)+".sh"
        try:
            p = Popen([bash_script_path, folder]+_files, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate()
            stdout = stdout.decode('ascii')
            stderr = stderr.decode('ascii')
            #out = check_output([bash_script_path, folder]+_files)
            if stdout:
                print(stdout)
                return _files
            else:
                print(stderr)
                return []

        except CalledProcessError as cpe:
            print('Error generating geotiff ' )

