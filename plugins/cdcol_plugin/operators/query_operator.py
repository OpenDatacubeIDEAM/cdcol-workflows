import os, errno
from airflow.models import BaseOperator
from airflow import utils as airflow_utils
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
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
    def __init__(self, execID, algorithm, version, product, lat, lon, time_ranges, params={}, to_tiff=False,
                 alg_folder=common.ALGORITHMS_FOLDER, *args, **kwargs):
        """
            algorithm: algorithm to execute over the query results
            version: algorithm version
            lat: (min_lat, max_lat)
            lon: (min_lon, max_lon)
            time_ranges: [(min_time, max_time)]
            product: datacube product to query
        """
        super(CDColQueryOperator, self).__init__(*args, **kwargs)
        self.execID = execID
        self.algorithm = algorithm
        self.version = version
        self.lat = lat
        self.lon = lon
        self.time_ranges = time_ranges
        self.alg_kwargs = params
        self.folder = "{}/{}/{}_{}/".format(common.RESULTS_FOLDER, execID, algorithm, version)
        self.product = product
        self.to_tiff = to_tiff
        self.alg_folder = alg_folder

    def execute(self, context):
        if not os.path.exists(os.path.dirname(self.folder)):
            try:
                os.makedirs(os.path.dirname(self.folder))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        folder = self.folder
        dc = datacube.Datacube(app=self.execID)
        kwargs = self.alg_kwargs
        xanm = "xarr"
        start = time.time()
        bands = []
        if self.product['bands'] != None and len(self.product['bands']) > 0:
            bands = self.product['bands']
        if isinstance(self.time_ranges, list) and self.alg_folder == common.COMPLETE_ALGORITHMS_FOLDER:
            i = 0
            for t in self.time_ranges:
                kwargs[xanm + str(i)] = dc.load(product=self.product['name'], measurements=bands, longitude=self.lon,
                                                latitude=self.lat, time=t)
                if len(kwargs[xanm + str(i)].data_vars) == 0:
                    print("ERROR: NO HAY DATOS EN LA ZONA")
                    open(posixpath.join(common.LOGS_FOLDER, self.execID, self.task_id, "no_data.lock"), "w+").close()
                    raise AirflowSkipException("No hay datos en la zona")
                i += 1
        else:
            kwargs[xanm + str(0)] = dc.load(product=self.product['name'], measurements=bands, longitude=self.lon,
                                            latitude=self.lat, time=self.time_ranges)
            if len(kwargs[xanm + str(0)].data_vars) == 0:
                print("ERROR: NO HAY DATOS EN LA ZONA")
                open(posixpath.join(common.LOGS_FOLDER, self.execID, self.task_id, "no_data.lock"), "w+").close()
                raise AirflowSkipException("No hay datos en la zona")
        # kwargs[xanm] = dc.load(product=self.product['name'], longitude=self.lon, latitude=self.lat, time=self.time_ranges)


        dc.close()
        end = time.time()
        logging.info('TIEMPO CONSULTA:' + str((end - start)))
        kwargs["product"] = self.product
        kwargs["folder"] = folder
        path = posixpath.join(self.alg_folder, self.algorithm, self.algorithm + "_" + str(self.version) + ".py")
        exec(open(path, encoding='utf-8').read(), kwargs)
        fns = []

        history = u'Creado con CDCOL con el algoritmo {} y  ver. {}'.format(self.algorithm, str(self.version))
        if "output" in kwargs:  # output debería ser un xarray
            # Guardar a un archivo...

            output = kwargs["output"]
            if self.to_tiff:
                filename = folder + "{}_{}_{}_{}_{}_output.tif".format(self.task_id, str(self.algorithm), self.lat[0],
                                                                       self.lon[0],
                                                                       re.sub('[^\w_.)(-]', '', str(self.time_ranges)))
                common.write_geotiff_from_xr(filename, output)
            else:
                filename = folder + "{}_{}_{}_{}_{}_output.nc".format(self.task_id, str(self.algorithm), self.lat[0],
                                                                      self.lon[0],
                                                                      re.sub('[^\w_.)(-]', '', str(self.time_ranges)))
                common.saveNC(output, filename, history)
            fns.append(filename)
        if "outputs" in kwargs:

            if self.to_tiff:

                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}_{}_{}_{}_{}.tif".format(self.task_id, str(self.algorithm), self.lat[0],
                                                                       self.lon[0],
                                                                       re.sub('[^\w_.)(-]', '', str(self.time_ranges)),
                                                                       xa)
                    common.write_geotiff_from_xr(filename, kwargs["outputs"][xa])
                    fns.append(filename)
            else:
                for xa in kwargs["outputs"]:
                    filename = folder + "{}_{}_{}_{}_{}_{}.nc".format(self.task_id, str(self.algorithm), self.lat[0],
                                                                      self.lon[0],
                                                                      re.sub('[^\w_.)(-]', '', str(self.time_ranges)),
                                                                      xa)
                    common.saveNC(kwargs["outputs"][xa], filename, history)
                    fns.append(filename)
        if "outputtxt" in kwargs:
            filename = folder + "{}_{}_{}.txt".format(self.lat[0], self.lon[0],
                                                      re.sub('[^\w_.)(-]', '', str(self.time_ranges)))
            with open(filename, "w") as text_file:
                text_file.write(kwargs["outputtxt"])
            fns.append(filename)
        if "outputxcom" in kwargs:
            fns.append(kwargs["outputxcom"])
        return fns;
