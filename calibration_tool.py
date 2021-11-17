###############################################################################
# Woking Calibration Tool                                                     #
# Designed for Woking Greens Project's Network.                               #
# Version: 1.0                                                                #
#                                                                             #
# This script is made to auto-calibrate the particulate matter data.          #
# Its basic use consists in run the script inside a folder that contains      #
# data from all sensors. It generate the file "data_calibrated.nc".           #
# This file is in NETCDF format, ideal to be used with the "xarray" library,  #
# and in many other applications.                                             #
#                                                                             #
# The required libraries outside base python are:                             #
# "numpy" and "xarray"                                                        #
#                                                                             #
# Woking Calibration Tool © 2021 by Leonardo Yoshiaki Kamigauti is licensed   #
# under Attribution-ShareAlike 4.0 International. To view a copy of this      #
# license, visit http://creativecommons.org/licenses/by-sa/4.0/               #
###############################################################################

import numpy as np
import glob
import re
from io import StringIO
from copy import deepcopy
import xarray as xr


def open_calibration_data(path):
    with open(path, 'r') as fp:
        calibration_params = json.load(fp, object_hook=deconvert)
    return calibration_params


def make_da(filepaths, save=False):
    da_list = []
    for filepath in filepaths:
        file_raw = open(filepath, 'r').read()
        pattern = re.compile('[^"]+')
        station = pattern.search(file_raw[:file_raw.index("\n")]).group()
        df = pd.read_csv(StringIO(file_raw),
                         skiprows=6,
                         names=['time', 'aqi', 'h_aqi', 'pm1', 'h_pm1', 'pm25', 'h_pm25', 'pm10', 'h_pm10', 'temp',
                                'h_temp', 'l_temp', 'rh', 'h-rh', 'l-rh', 'dew', 'h_dew', 'l_dew', 'wetbulb',
                                'h_wetbulb', 'l_wetbulb', 'heatindex', 'h_heatindex'],
                         parse_dates=['time'],
                         index_col='time')
        da = df.to_xarray().to_array()
        da['station'] = station
        da = da.expand_dims(dim='station')
        da_list.append(da)
    da = xr.concat(da_list, dim='station')
    da_clear = convert_to_float_and_replace_nan(da, deep_copy=True)
    if save:
        da_clear.to_netcdf(DATA_DIR + 'data.nc')
    return da_clear


def convert_to_float_and_replace_nan(da, deep_copy=False, precision=32):
    if deep_copy:
        da = da.copy()

    data_temp = da.values.copy()

    original_shape = deepcopy(data_temp.shape)

    data_temp = data_temp.flatten()

    for idx, value in enumerate(data_temp):
        try:
            data_temp[idx] = float(value)
        except ValueError:
            data_temp[idx] = np.nan
    data_temp = data_temp.reshape(original_shape)
    da = da.copy(data=data_temp)
    da = da.astype(f'float{precision}')
    return da


def calibrator(data, target, calibration_params):
    X = data.sel(variable=[target, 'rh', 'temp']).values.copy()
    if data.station.values.shape == ():
        station_ = data.station.values.tolist()
        y = calibration.calibrate(X.transpose(), calibration_params[target][station_]).copy()
    else:
        station_ = data.station.values[0]
        y = calibration.calibrate(X[0].transpose(), calibration_params[target][station_]).copy()
    da = xr.DataArray(
        y.reshape(-1, 1),
        coords=[('time', data.time.values.copy()), ('variable', [target+'_cal'])])
    da = da.astype('float32')
    return da

def make_calibration(data, DATA_DIR, save=False):
    calibration_params = open_calibration_data(DATA_DIR + 'calibration_parameters.json')
    da_calibrated = data.copy().rename('calibrated')
    for pm in ['pm10', 'pm25', 'pm1']:
        cal = da_calibrated.groupby('station').map(calibrator, args=(pm, calibration_params)).copy().rename('case')
        da_calibrated = xr.concat([da_calibrated, cal], dim='variable')
    if save:
        da_calibrated.to_netcdf(DATA_DIR + 'data_calibrated.nc')
    return(da_calibrated)

if __name__ == '__main__':
    # DATA_DIR = 'G:/My Drive/IC/Doutorado/Sandwich/Data/' # if you want to run the script from another directory
    filepaths = glob.glob(DATA_DIR + 'WokingGreens*')
    da = make_da(filepaths)
    # da = xr.open_dataarray(DATA_DIR + 'data.nc') # case the lcs.nc is already saved
    da = make_calibration(da, DATA_DIR, save=True)
