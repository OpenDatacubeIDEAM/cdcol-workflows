#!/usr/bin/python3
# coding=utf8
import os, errno
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from cdcol_plugin.operators import common
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException


#This class define the CDColFromFileOperator, that will be used to make an Identity Mapper
#Each map task of the identity mapper is a CDColFromFileOperator
class CDColFromFileOperator(BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, execID, algorithm,version, product,str_files=None, lat=None, lon=None, output_type="output",params={},  to_tiff=False, *args,**kwargs):
        """
            algorithm: algorithm to execute over the query results
            version: algorithm version
            product: datacube product used from previous steps to generate input
            str_file: 
        """
        super(CDColFromFileOperator,self).__init__(*args, **kwargs)
        self.execID = execID
        self.algorithm = algorithm
        self.version = version
        self.str_files = str_files
        self.lat=lat
        self.lon=lon
        self.alg_kwargs=params
        self.folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID,algorithm,version,)
        self.product = product
        self.output_type=output_type
        self.to_tiff = to_tiff
     
    def execute(self, context):
        if not os.path.exists(os.path.dirname(self.folder)):
            try:
                os.makedirs(os.path.dirname(self.folder))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        folder=self.folder
        if self.str_files is None:
            self.str_files=common.getUpstreamVariable(self, context)
        if self.str_files is None or len(self.str_files) == 0:
            raise AirflowSkipException("there are not files")
        i=0

        _nc_files = [x for x in self.str_files if ".nc" in x]
        _files=[ x for x in self.str_files if self.output_type in x]
        _other_files = [x for x in self.str_files if ".nc" not in x]

        kwargs=self.alg_kwargs
        for _f in _files:
            xanm="xarr"+str(i)
            kwargs[xanm] = common.readNetCDF(_f)
            i+=1
            if len(kwargs[xanm].data_vars) == 0:
                raise AirflowSkipException("No data inside the files ")
        
        kwargs["product"]=self.product
        kwargs["folder"] = folder
        kwargs["other_files"] = _other_files
        exec(open(common.ALGORITHMS_FOLDER+"/"+self.algorithm+"/"+self.algorithm+"_"+str(self.version)+".py", encoding='utf-8').read(),kwargs)
        fns=[]

        history = u'Creado con CDCOL con el algoritmo {} y  ver. {}'.format(self.algorithm,str(self.version))
        _fn=os.path.basename(_files[0])
        if "output" in kwargs: #output debería ser un xarray
            #Guardar a un archivo...

            output=  kwargs["output"]
            if self.to_tiff:
                # filename = folder + "{}_{}_{}_{}_{}_output.tif".format(self.task_id, str(self.algorithm),_fn.split("_")[2], _fn.split("_")[3],_fn.split("_")[4])
                # common.write_geotiff_from_xr(filename, output)
                filename = folder + "{}_{}_{}_{}_{}_output.nc".format(self.task_id, str(self.algorithm),
                                                                      _fn.split("_")[2], _fn.split("_")[3],
                                                                      _fn.split("_")[4])
                common.saveNC(output, filename, history)
                common.translate_netcdf_to_tiff(self.task_id, str(self.algorithm), self.folder,[filename])
            else:
                filename = folder + "{}_{}_{}_{}_{}_output.nc".format(self.task_id, str(self.algorithm),
                                                                      _fn.split("_")[2], _fn.split("_")[3],
                                                                      _fn.split("_")[4])
                common.saveNC(output,filename, history)
            fns.append(filename)
        if "outputs" in kwargs:
            if self.to_tiff:
                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}_{}_{}_{}_{}.tif".format(self.task_id, str(self.algorithm),
                                                                      _fn.split("_")[2], _fn.split("_")[3],
                                                                      _fn.split("_")[4], xa)
                    common.write_geotiff_from_xr(filename, kwargs["outputs"][xa])
                    fns.append(filename)

            else:
                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}_{}_{}_{}_{}.nc".format(self.task_id, str(self.algorithm),
                                                                      _fn.split("_")[2], _fn.split("_")[3],
                                                                      _fn.split("_")[4], xa)
                    common.saveNC(kwargs["outputs"][xa], filename, history)
                    fns.append(filename)
        if "outputtxt" in kwargs:
            filename=folder+"{}_{}_{}_{}_{}.txt".format(self.task_id,str(self.algorithm),_fn.split("_")[2],_fn.split("_")[3],_fn.split("_")[4])
            with open(filename, "w") as text_file:
                text_file.write(kwargs["outputtxt"])
            fns.append(filename)
        if "outputxcom" in kwargs:
            fns.append(kwargs["outputxcom"])
        return fns;