#!/usr/bin/python3
# coding=utf8
import os,errno
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from cdcol_plugin.operators import common
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException

#This class define the CDColReduceOperator, that will be used to make an reducer
#Each reduce task of the reducer is a CDColReduceOperator
class CDColReduceOperator(BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, execID, algorithm,version, product, params={}, str_files=None, output_type="output", lat=None, lon=None,year=None, to_tiff=False, *args,**kwargs):
        """
            algorithm: algorithm to execute over the query results
            version: algorithm version
            product: datacube product used from previous steps to generate input
            str_file: 
        """
        super(CDColReduceOperator,self).__init__(*args, **kwargs)
        self.execID = execID
        self.algorithm = algorithm
        self.version = version
        self.str_files = str_files
        self.alg_kwargs=params
        self.folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID,algorithm,version,)
        self.product = product
        self.output_type=output_type
        self.lat=lat
        self.lon=lon
        self.year=year
        self.to_tiff=to_tiff
     
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
            raise AirflowSkipException("there is not files")
        i=0
        _nc_files=[x for x in self.str_files if ".nc" in x]
        _files=[ x for x in _nc_files if "{}.nc".format(self.output_type) in x and (self.lat is None or "{}_{}".format(self.lat[0],self.lon[0]) in x) and self.year is None or "_{}_".format(self.year)]
        _other_files=[x for x in self.str_files if ".nc" not in x]
        print(_nc_files)
        print(_files)
        print(_other_files)
        kwargs=self.alg_kwargs
        xarrs={}
        for _f in _files:
            
            _xarr = common.readNetCDF(_f)
            if len(_xarr.data_vars) == 0:
                raise AirflowSkipException("No data inside the files ")
            xarrs[os.path.basename(_f)]=_xarr
        kwargs["xarrs"]=xarrs
        kwargs["product"]=self.product
        kwargs["folder"]=folder
        kwargs["other_files"]=_other_files
        exec(open(common.ALGORITHMS_FOLDER+"/"+self.algorithm+"/"+self.algorithm+"_"+str(self.version)+".py", encoding='utf-8').read(),kwargs)
        fns=[]

        history = u'Creado con CDCOL con el algoritmo {} y  ver. {}'.format(self.algorithm,str(self.version))
        if not self.lat is None and not self.year is None: 
            _exp="{}_{}_{}_{}_{}".format(self.task_id,str(self.algorithm),self.lat[0],self.lon[0],self.year )
        elif not self.lat is None:
            _exp="{}_{}_{}_{}_all".format(self.task_id,str(self.algorithm),self.lat[0],self.lon[0] )
        elif not self.year is None: 
            _exp="{}_{}_{}_{}_{}".format(self.task_id,str(self.algorithm),"All","All",self.year )
        else:
            _exp="{}_{}_{}_{}_{}".format(self.task_id,str(self.algorithm),"All","All","All" )


        if "output" in kwargs: #output debería ser un xarray
            #Guardar a un archivo...
            filename=folder+"{}_output.nc".format(_exp )
            output=  kwargs["output"]
            if self.to_tiff:
                common.write_geotiff_from_xr(filename, output)
            else:
                common.saveNC(output,filename, history)
            fns.append(filename)
        if "outputs" in kwargs:
            if self.to_tiff:
                common.write_geotiff_from_xr(filename, ouput, bands)
                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}.nc".format(_exp, xa)
                    common.write_geotiff_from_xr(filename, kwargs["outputs"][xa])
                    fns.append(filename)
            else:
                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}.nc".format(_exp, xa)
                    common.saveNC(kwargs["outputs"][xa], filename, history)
                    fns.append(filename)
        if "outputtxt" in kwargs:
            filename=folder+"{}.txt".format(_exp)
            with open(filename, "w") as text_file:
                text_file.write(kwargs["outputtxt"])
            fns.append(filename)
        if "outputxcom" in kwargs:
            fns.append(kwargs["outputxcom"])
        return fns;