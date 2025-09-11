import functools

import xarray
import rioxarray as rio


# This function is to use to keep spatial information of the datacube
def rio_decorator(func):
    @functools.wraps(func)
    def keep_spatial_ref(*args, **kwargs):
        # Do some calculation
        result = func(*args, **kwargs)

        # Copy spatial information using rioxarray
        result.rio.write_transform(args[0].rio.transform(), inplace=True)
        result.rio.write_crs(args[0].rio.crs, inplace=True)
        result.rio.update_attrs(args[0].attrs, inplace=True)
        result.rio.update_encoding(args[0].encoding, inplace=True)

        # Copy spatial information of every variable if the input is an xarray Dataset
        if isinstance(result, xarray.Dataset):
            # Assign the spatial information from the first variable of input dataset
            _data = args[0][list(args[0].keys())[0]]
            for var in result:
                result[var].rio.write_transform(_data.rio.transform(), inplace=True)
                result[var].rio.write_crs(_data.rio.crs, inplace=True)
                result[var].rio.update_attrs(_data.attrs, inplace=True)
                result[var].rio.update_encoding(_data.encoding, inplace=True)
        return result
    return keep_spatial_ref


@rio_decorator
def median(ds):
    ds = ds.chunk({'time': -1})
    median = ds.quantile(0.5, dim='time', method='nearest', keep_attrs=True)
    return median


@rio_decorator
def max(ds):
    ds = ds.chunk({'time': -1})
    max = ds.max(dim='time', keep_attrs=True)
    return max


@rio_decorator
def ndvi(ds):
    ndvi = (ds.nbart_nir_1 - ds.nbart_red) / (ds.nbart_nir_1 + ds.nbart_red)
    ds['ndvi'] = ndvi.astype('float32')
    return ds
