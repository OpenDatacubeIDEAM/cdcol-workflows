import xarray as xr
import glob, os,sys

# folder=sys.argv[1]
# postfix=sys.argv[2]
# os.chdir(folder)
# output=None
# for file in glob.glob("*_{}".format(postfix)):
#     print output
#     if(output is None):
#         output=xr.open_dataset(file)
#     else:
#         output=output.combine_first(xr.open_dataset(file))
# print output
#
#
# # In[6]:

# output.to_netcdf('mosaico_xarr_{}'.format(postfix))

output=None
for _xarr in xarrs:
    if (output is None):
        output = _xarr
    else:
        output=output.combine_first(_xarr)
