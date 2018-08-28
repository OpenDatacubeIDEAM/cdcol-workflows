# coding=utf8
import os
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from cdcol_plugin.operators import common
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException


#This class define the CDColFromFileOperator, that will be used to make an Identity Mapper
#Each map task of the identity mapper is a CDColFromFileOperator
class CDColFromFileOperator(BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, execID, algorithm,version, product,str_files=None, lat=None, lon=None, output_type="output",params={}, *args,**kwargs):
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
        _files=[ x for x in self.str_files if self.output_type in x]
        kwargs=self.alg_kwargs
        for _f in _files:
            xanm="xarr"+str(i)
            kwargs[xanm] = common.readNetCDF(_f)
            i+=1
            if len(kwargs[xanm].data_vars) == 0:
                open(folder+"{}_{}_no_data.lock".format(self.lat[0],self.lon[0]), "w+").close()
                return []
        
        kwargs["product"]=self.product
        exec(open(common.ALGORITHMS_FOLDER+"/"+self.algorithm+"/"+self.algorithm+"_"+str(self.version)+".py").read(),kwargs)
        fns=[]

        history = u'Creado con CDCOL con el algoritmo {} y  ver. {}'.format(self.algorithm,str(self.version))
        _fn=os.path.basename(_files[0])
        if "output" in kwargs: #output debería ser un xarray
            #Guardar a un archivo...
            filename=folder+"{}_{}_{}_{}_{}_output.nc".format(self.task_id,str(self.algorithm),_fn.split("_")[2],_fn.split("_")[3],_fn.split("_")[4])
            output=  kwargs["output"]
            print(filename)
            common.saveNC(output,filename, history)
            fns.append(filename)
        if "outputs" in kwargs:
            for xa in kwargs["outputs"]:
                filename=folder+"{}_{}_{}_{}_{}_{}.nc".format(self.task_id,str(self.algorithm),_fn.split("_")[2],_fn.split("_")[3],_fn.split("_")[4],xa)
                common.saveNC(kwargs["outputs"][xa],filename, history)
                fns.append(filename)
        if "outputtxt" in kwargs:
            filename=folder+"{}_{}_{}_{}_{}.txt".format(self.task_id,str(self.algorithm),_fn.split("_")[2],_fn.split("_")[3],_fn.split("_")[4])
            with open(filename, "w") as text_file:
                text_file.write(kwargs["outputtxt"])
            fns.append(filename)
        return fns;