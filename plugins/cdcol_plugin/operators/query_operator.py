import os, errno
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from cdcol_plugin.operators import common
import datacube
import re
import posixpath
import time
import logging

logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
logging.getLogger().addHandler(logging.StreamHandler())

class CDColQueryOperator(BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, execID, algorithm,version, product, lat, lon, time_ranges, params={}, *args,**kwargs):
        """
            algorithm: algorithm to execute over the query results
            version: algorithm version
            lat: (min_lat, max_lat)
            lon: (min_lon, max_lon)
            time_ranges: [(min_time, max_time)]
            product: datacube product to query
        """
        super(CDColQueryOperator,self).__init__(*args, **kwargs)
        self.execID = execID
        self.algorithm = algorithm
        self.version = version
        self.lat = lat
        self.lon = lon
        self.time_ranges = time_ranges
        self.alg_kwargs=params
        self.folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID,algorithm,version)
        self.product = product
     
    def execute(self, context):
        if not os.path.exists(os.path.dirname(self.folder)):
            try:
                os.makedirs(os.path.dirname(self.folder))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        folder=self.folder
        dc = datacube.Datacube(app=self.execID)
        i=0
        kwargs=self.alg_kwargs
        xanm="xarr0"


        start = time.time()
        # if kwargs['bands']:
        #     kwargs[xanm] = dc.load(product=self.product, measurements=kwargs['bands'], longitude=self.lon, latitude=self.lat, time=self.time_ranges)
        # else:
        #     kwargs[xanm] = dc.load(product=self.product, longitude=self.lon, latitude=self.lat, time=self.time_ranges)
        kwargs[xanm] = dc.load(product=self.product, longitude=self.lon, latitude=self.lat, time=self.time_ranges)

        if len(kwargs[xanm].data_vars) == 0:
            raise AirflowSkipException("No hay datos en la zona")
        dc.close()
        end = time.time()
        logging.info('TIEMPO CONSULTA:' + str((end - start)))
        kwargs["product"]=self.product
        print(common.ALGORITHMS_FOLDER+"/"+self.algorithm+"/"+self.algorithm+"_"+str(self.version)+".py")
        path = posixpath.join(common.ALGORITHMS_FOLDER,self.algorithm,self.algorithm+"_"+str(self.version)+".py")
        exec(open(path, encoding='utf-8').read(),kwargs)
        fns=[]

        history = u'Creado con CDCOL con el algoritmo {} y  ver. {}'.format(self.algorithm,str(self.version))
        if "output" in kwargs: #output debería ser un xarray
            #Guardar a un archivo...
            filename=folder+"{}_{}_{}_{}_{}_output.nc".format(self.task_id,str(self.algorithm),self.lat[0],self.lon[0],re.sub('[^\w_.)(-]', '', str(self.time_ranges)))
            output=  kwargs["output"]
            common.saveNC(output,filename, history)
            fns.append(filename)
        if "outputs" in kwargs:
            for xa in kwargs["outputs"]:
                filename=folder+"{}_{}_{}_{}_{}_{}.nc".format(self.task_id,str(self.algorithm),self.lat[0],self.lon[0],re.sub('[^\w_.)(-]', '', str(self.time_ranges)),xa)
                common.saveNC(kwargs["outputs"][xa],filename, history)
                fns.append(filename)
        if "outputtxt" in kwargs:
            filename=folder+"{}_{}_{}.txt".format(self.lat[0],self.lon[0],re.sub('[^\w_.)(-]', '', str(self.time_ranges)))
            with open(filename, "w") as text_file:
                text_file.write(kwargs["outputtxt"])
            fns.append(filename)
        return fns;