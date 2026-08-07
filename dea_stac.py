# Disable non-fatal warnings
import warnings

import rasterio.errors
warnings.filterwarnings("ignore")

import argparse
from argparse import RawTextHelpFormatter
import inspect
from pathlib import Path
import time
import concurrent.futures
import json
from html.parser import HTMLParser
import tempfile
import logging
import datetime
import functools
import re

import pystac_client
from odc.geo import xr, geobox
from odc.geo.data import country_geom
import odc.geo.geom
import odc.stac
import dask
import dask.distributed
import geopandas as gpd
import shapely
import shapely.geometry
import xarray
import rioxarray as rio
import numpy as np
import rasterio
from pyproj import CRS
import scipy.ndimage as ndi

from . import variables
from . import algs


dea_stac_url = 'https://explorer.dea.ga.gov.au/stac'
size_limit = 2000

TASK_LIMIT = 5


class MyHTMLParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.recording = 0
        self.data = list()
    def handle_starttag(self, tag, attrs):
        if tag == 'title':
            self.recording = 1
    def handle_endtag(self, tag):
        if tag == 'title':
            self.recording -= 1
    def handle_data(self, data):
        if self.recording:
            self.data.append(data)


class VerifyNoBbox(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        # print 'No: {n} {v} {o}'.format(n=args, v=values, o=option_string)
        if args.bbox is not None:
            parser.error(
                '--bbox should not be used with --vector')
        setattr(args, self.dest, values)


class VerifyNoVector(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        # print 'No: {n} {v} {o}'.format(n=args, v=values, o=option_string)
        if args.vector is not None:
            parser.error(
                '--vector should not be used with --bbox')
        setattr(args, self.dest, values)


class VerifyTilesize(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        # print 'No: {n} {v} {o}'.format(n=args, v=values, o=option_string)
        if values < 5:
            parser.error(
                '--tile_size value should be >= 5.')
        setattr(args, self.dest, values)


class VerifyOverlap(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        # print 'No: {n} {v} {o}'.format(n=args, v=values, o=option_string)
        if args.tile_size is None and values is not None:
            parser.error(
                '--overlap should be used with --tile_size')
        elif values < 0 or values > 80:
            parser.error(
                '--overlap value should be between 0 and 80.')
        setattr(args, self.dest, values)


def get_stac_pages(collections, geom, date_start, date_end):
    while True:
        try: 
            # Sleep for 200 milliseconds to avoid abusing web server
            time.sleep(0.2)

            catalog = pystac_client.Client.open(dea_stac_url)

            # Build a query with the set parameters
            query = catalog.search(max_items=None, 
                                   limit=100, 
                                   intersects=geom, 
                                   collections=collections, 
                                   datetime=f"{date_start}/{date_end}")

            # Retrieve results from pages
            print('Paginating the results...')
            pages = list(query.pages())
            
            return pages
        
        except pystac_client.exceptions.APIError as e:
            # Retry if encountering API error
            htmlparser = MyHTMLParser()
            htmlparser.feed(str(e))

            try:
                print(f'API error: \n{htmlparser.data[0]}')
            except IndexError:
                print(f'API error: {e}')

            print('Retry in 30 seconds.')
            time.sleep(30)

        except json.decoder.JSONDecodeError as e:
            # Retry if there is error when decoding the response
            print(f'error: \n{e}')
            print('Retry in 30 seconds.')
            time.sleep(30)


def get_stac_items(pages):
    def retrieve_page(page):
        try:
            # Sleep for 200 milliseconds to avoid abusing web server
            time.sleep(0.2)

            items = page.items
            print(f'Retrieving {len(items)} items.')
            return items
        except pystac_client.exceptions.APIError as e:
            # Retry the same page if encountering API error. 
            htmlparser = MyHTMLParser()
            htmlparser.feed(str(e))

            print(f'API error: \n{htmlparser.data[0]}')
            print('Retry in 30 seconds.')
            time.sleep(30)
            return retrieve_page(page)
    
    
    items = list()

    # Maximum thread number set to task limit to avoid spamming the server
    with concurrent.futures.ThreadPoolExecutor(max_workers=TASK_LIMIT) as executor:
        items = list(executor.map(retrieve_page, pages))

    # Get the items which scene cloud cover is below 60%
    try:
        items = [item for sublist in items for item in sublist if item.properties['eo:cloud_cover'] <= 60]
    # Skip the cloud cover check if there is no such property
    except KeyError:
        items = [item for sublist in items for item in sublist]

    return items


def load_as_datacube(query_items, geom_geobox, geom, query_bands, rename=False, fail_on_error=True):
    # The operation is under UTM in order to get homogeneous spatial resolution
    ds = odc.stac.load(
                       query_items,
                       bands=query_bands,
                       chunks={'time': 'auto', 'x': 'auto', 'y': 'auto'},
                       dtype='float32', 
                       groupby="solar_day",
                       geobox=geom_geobox, 
                       fail_on_error=fail_on_error
                       )
    # Rename Landsat bands to match Sentinel band names
    if rename:
        ds = ds.rename({_key: _item for _key, _item in variables.lsband_rename_table.items() if _key in ds})

    # Convert dtype for all variables to int16
    for var in ds: 
        _data = ds[var]
        if 'nbart' in var:
            ds[var] = _data.astype('int16')
        ds[var].rio.write_transform(_data.rio.transform(), inplace=True)
        ds[var].rio.write_crs(_data.rio.crs, inplace=True)
        ds[var].rio.update_attrs(_data.attrs, inplace=True)
        ds[var].rio.update_encoding(_data.encoding, inplace=True)
    
    if geom.is_empty:
        # Create a dataMask as what SentinelHub provided (0=nodata, 1=valid)
        datamask = xarray.zeros_like(ds[query_bands[0]])

        ds['dataMask'] = datamask

        # Create a geom layer to mark the boundary of the data
        ds['geom'] = datamask

    else:
        # Create a dataMask as what SentinelHub provided (0=nodata, 1=valid)
        datamask = xarray.ones_like(ds[query_bands[0]])
        
        ds['dataMask'] = datamask

        # Create a geom layer to mark the boundary of the data
        ds['geom'] = datamask

        # Rasterise the geometry
        geom_rasterised = xr.rasterize(geom, geom_geobox)

        # Mask out the data outside geometry
        ds['dataMask'] = ds.dataMask.where(geom_rasterised)
        ds['geom'] = ds.geom.where(geom_rasterised)

        # Cloud masking
        ds['dataMask'] = cloud_mask(ds)

    # Copy spatial information using rioxarray
    ds['dataMask'] = ds.dataMask.fillna(0).astype('int8')
    ds['dataMask'].rio.write_crs(ds[query_bands[0]].rio.crs, inplace=True)
    ds['dataMask'].rio.update_attrs(ds[query_bands[0]].attrs, inplace=True)
    ds['dataMask'].rio.update_encoding(ds[query_bands[0]].encoding, inplace=True)
    ds['geom'] = ds.geom.fillna(0).astype('int8')
    ds['geom'].rio.write_crs(ds[query_bands[0]].rio.crs, inplace=True)
    ds['geom'].rio.update_attrs(ds[query_bands[0]].attrs, inplace=True)
    ds['geom'].rio.update_encoding(ds[query_bands[0]].encoding, inplace=True)

    # Normalise time to date precision
    ds['time'] = ds.indexes['time'].round('D')
    
    return ds


def cloud_mask(ds):
    datamask = ds.dataMask
    spatial_dims = ds.odc.spatial_dims
    
    # Mask cloud with s2cloudless
    try:
        cloudmask_s2cloudless = (ds.oa_s2cloudless_prob >= 0.35).astype(bool)
        cloudmask_fmask = (ds.oa_fmask == 2).astype(bool)
    # Mask cloud with fmask if no s2cloudless
    except AttributeError:
        cloudmask_fmask = (ds.oa_fmask == 2).astype(bool)

    # Cloud mask with Braaten-Cohen-Yang cloud detector
    bRatio = (ds.nbart_green - 1750) / (3900 - 1750)
    NDGR = (ds.nbart_green - ds.nbart_red) / (ds.nbart_green + ds.nbart_red)
    cloudmask_bcy_1 = (np.logical_and(ds.nbart_swir_2 > 1000, bRatio > 1)).astype(bool)
    cloudmask_bcy_2 = (np.logical_and(np.logical_and(ds.nbart_swir_2 > 1000, bRatio > 0), NDGR > 0)).astype(bool)
    cloudmask_bcy = cloudmask_bcy_1 | cloudmask_bcy_2

    # Combine different cloudmask
    try:
        cloudmask = cloudmask_s2cloudless | cloudmask_fmask | cloudmask_bcy
        del cloudmask_s2cloudless

    except UnboundLocalError:
        cloudmask = cloudmask_fmask | cloudmask_bcy

    # Mask shadow with fmask
    shadow_fmask = (ds.oa_fmask == 3).astype(bool)

    # Find potential cloud shadow by calculating cloud projection
    x_res, y_res = map(abs, ds.rio.resolution())
    shadow_proj = xarray.apply_ufunc(find_shadow, 
                                     cloudmask, 
                                     ds.oa_fmask, 
                                     ds.nbart_nir_1, 
                                     ds.oa_solar_azimuth, 
                                     input_core_dims=[spatial_dims, spatial_dims, spatial_dims, spatial_dims], 
                                     output_core_dims=[spatial_dims], 
                                     kwargs={'x_res': x_res, 'y_res': y_res}, 
                                     vectorize=True, 
                                     dask='parallelized')

    # Combine two shadow mask
    shadow = shadow_fmask | shadow_proj
    cloud_and_shadow = cloudmask | shadow

    # Clean up the cloud and shadow mask
    cloud_and_shadow = xarray.apply_ufunc(cleanup_mask, 
                                          cloud_and_shadow, 
                                          input_core_dims=[spatial_dims], 
                                          output_core_dims=[spatial_dims], 
                                          kwargs={'x_res': x_res, 'y_res':y_res}, 
                                          vectorize=True, 
                                          dask='parallelized')

    # Apply cloud and shadow mask to dataMask
    datamask = datamask.where(~cloud_and_shadow)

    # Fill NAN with 0
    datamask = datamask.fillna(0).astype(int)

    return datamask


def find_shadow(cloudmask, oa_fmask, nir, solar_azimuth, x_res, y_res, max_distance=2000):
    # Identify water pixels from the fmask
    not_water = (oa_fmask != 5).astype(bool)

    # Identify dark NIR pixels that are not water (potential cloud shadow pixels).
    dark_pixels = (nir < 2500).astype(bool) & not_water

    # Get the cloud projection
    mean_solar_azimuth = np.nanmean(solar_azimuth)

    # Skip the cloud projection calculation if there is no solar azimuth information
    if np.isnan(mean_solar_azimuth):
        shadows = np.zeros_like(cloudmask, dtype=bool)
        return shadows

    cloud_proj = cloud_projection(cloudmask.astype(bool), mean_solar_azimuth, x_res, y_res, max_distance)
    shadows = (cloud_proj & dark_pixels).astype(bool)

    return shadows


def cloud_projection(cloudmask, solar_azimuth, x_res, y_res, max_distance=2000):
    # Convert to radians (north-based azimuth → math system)
    theta = np.deg2rad(90 - solar_azimuth)

    # Number of steps = max pixel displacement
    max_dx = abs(int(round((max_distance * np.cos(theta)) / x_res)))
    max_dy = abs(int(round((max_distance * np.sin(theta)) / y_res)))
    n_steps = max(max_dx, max_dy)

    proj = np.zeros_like(cloudmask, dtype=bool)

    for step in range(1, n_steps + 1):
        # displacement in meters
        dx_m = np.cos(theta) * (step * max_distance / n_steps)
        dy_m = np.sin(theta) * (step * max_distance / n_steps)

        # convert to pixels
        sx = int(round(dx_m / x_res))
        sy = int(round(dy_m / y_res))

        shifted = np.roll(cloudmask, shift=(sy, sx), axis=(0, 1))
        proj |= shifted.astype(bool)

    return proj


def cleanup_mask(mask, x_res, y_res, buffer_m=100):
    """
    Clean cloud+shadow mask by applying erosion and dilation

    Parameters
    ----------
    mask : np.ndarray (2D)
        Input boolean or uint8 mask (1=cloud/shadow, 0=clear).
    x_res, y_res : float
        Pixel resolution in meters (absolute values). Used to convert buffer_m to pixels.
    buffer_m : float
        Buffer distance in meters (default 100).

    Returns
    -------
    np.ndarray
        Cleaned mask (same shape as input).
    """
    # Ensure boolean
    mask_bool = mask.astype(bool)

    # Step 1: Erosion (remove small speckles, ~2 px radius)
    eroded = ndi.binary_erosion(mask_bool, structure=np.ones((3, 3)))

    # Step 2: Dilation (buffer by BUFFER meters)
    # Convert buffer to pixel size
    px_size = (abs(x_res) + abs(y_res)) / 2
    buffer_px = int(round(buffer_m / px_size))

    dilated = ndi.binary_dilation(eroded, structure=ndi.generate_binary_structure(2, 1), iterations=buffer_px)

    return dilated


def get_annual_dataset(args, date_start, date_end, geom_geobox, geom, selected_bands, fail_on_error=True):
        print(f'Searching data between {date_start} and {date_end}....')
        
        if geom.is_empty:
            pages = get_stac_pages(args.collection, geom_geobox.footprint('EPSG:4326'), date_start, date_end)
        else:
            pages = get_stac_pages(args.collection, geom, date_start, date_end)

        items = get_stac_items(pages)
        
        print(f'Found: {len(items)} datasets')

        # End the program if no items were found
        if len(items) == 0:
            return
        
        # Load Landsat and Sentinel dataset separately
        s2_items = [_item for _item in items if 'sentinel' in _item.properties['platform']]
        ls_items = [_item for _item in items if 'landsat' in _item.properties['platform']]
        ds_dict = dict()
        if len(s2_items) > 0:
            ds_dict['sentinel'] =  load_as_datacube(s2_items, geom_geobox, geom, selected_bands[0]['sentinel'], fail_on_error=fail_on_error)

        if len(ls_items) > 0:
            ds_dict['landsat'] =  load_as_datacube(ls_items, geom_geobox, geom, selected_bands[0]['landsat'], rename=True, fail_on_error=fail_on_error)
            
            # Use Sentinel data instead of Landsat data if the data are acquired on the same date
            if 'sentinel' in ds_dict.keys():
                ds_dict['landsat'] = ds_dict['landsat'].sel(time= ds_dict['landsat'].time[~np.isin(ds_dict['landsat'].time, ds_dict['sentinel'].time)])

        # Merge Landsat and Sentinel along time dimension
        ds = xarray.concat(ds_dict.values(), dim='time').sortby('time').chunk({'time': -1})

        return ds


def get_dataset(args, geom_geobox, geom, selected_bands, feature_id=None, tmp_dir=None, fail_on_error=True):
    # Check the length of the time series
    time_diff = datetime.date.fromisoformat(args.date_end) - datetime.date.fromisoformat(args.date_start)
    one_year = datetime.timedelta(days=365)

    # Make one request if the length is within 12 months
    if time_diff <= one_year:
        ds = get_annual_dataset(args, args.date_start, args.date_end, geom_geobox, geom, selected_bands, fail_on_error=fail_on_error)
        
        # Return None if no data
        if ds is None:
            return
        else:
            # Save spatial information using CF convention
            ds = save_crs(ds)

        if tmp_dir is not None:
            tmp_file = Path(tmp_dir) / f"{feature_id}_{'_'.join([str(pp) for pp in geom_geobox.boundingbox])}.zarr"
            # Compress the data with built-in zlib method
            comp = dict(zlib=True, complevel=2, fletcher32=True)
            
            for var in ds: 
                ds[var].encoding.update(comp)
                
            if len(args.algorithm) == 0:
                print(f'Saving temprary file to {tmp_file}')
                ds.to_zarr(tmp_file)
                print(f'{tmp_file} was saved.')
                ds.close()
                return

    # Request the data for every 12 months
    else:
        annualty = time_diff // one_year
        remaining = time_diff % one_year
        request_dates = [(datetime.date.fromisoformat(args.date_start) + an*one_year, 
                        datetime.date.fromisoformat(args.date_start) + an*one_year + one_year - datetime.timedelta(days=1)) 
                        for an in range(annualty)]
        last_date = request_dates[-1][-1]
        request_dates.append((last_date+datetime.timedelta(days=1), last_date+datetime.timedelta(days=1)+remaining))
        # Convert the dates to ISO format (YYYY-MM-DD)
        request_dates = [(d1.isoformat(), d2.isoformat()) for d1, d2 in request_dates]

        if tmp_dir is None:
            ds_list = list()
            for date_start, date_end in request_dates:
                _ds = get_annual_dataset(args, date_start, date_end, geom_geobox, geom, selected_bands, fail_on_error=fail_on_error)
                if _ds is not None:
                    ds_list.append(_ds)

            # Return None if no data
            if len(ds_list) == 0:
                print('No dataset was found.')
                return
            
            # Merge the datasets from different time range
            print('Combining the datasets from different time range...')
            ds = xarray.concat(ds_list, dim='time')
            # Save spatial information using CF convention
            ds = save_crs(ds)

        else:
            with tempfile.TemporaryDirectory(dir=tmp_dir) as time_slice_dir:
                for date_start, date_end in request_dates:
                    _ds = get_annual_dataset(args, date_start, date_end, geom_geobox, geom, selected_bands, fail_on_error=fail_on_error)
                    if _ds is not None:
                        tmp_file = Path(time_slice_dir) / f"{feature_id}_{'_'.join([str(pp) for pp in geom_geobox.boundingbox])}_{date_start.replace('-', '')}_{date_end.replace('-', '')}.zarr"
                        # Compress the data with built-in zlib method
                        comp = dict(zlib=True, complevel=2, fletcher32=True)

                        # Save spatial information using CF convention
                        _ds = save_crs(_ds)
                        
                        for var in _ds: 
                            _ds[var].encoding.update(comp)
                        print(f'Saving temprary file to {tmp_file}')
                        _ds.to_zarr(tmp_file)
                        print(f'{tmp_file} was saved.')
                        _ds.close()
                
                print('Combining the datasets from different time range...')
                
                # Delayed loading the dataset with dask chunks
                try:
                    ds = xarray.open_mfdataset(Path(time_slice_dir).glob(f"{feature_id}_{'_'.join([str(pp) for pp in geom_geobox.boundingbox])}_*.zarr"), 
                                            engine='zarr', parallel=True).chunk({'time': -1})
                    
                except OSError:
                    print('No dataset was found.')
                    return
                
                # Save spatial information using CF convention
                ds = update_dtype(ds, args)
                ds = save_crs(ds)

                if tmp_dir:
                    tmp_file = Path(tmp_dir) / f"{feature_id}_{'_'.join([str(pp) for pp in geom_geobox.boundingbox])}.zarr"
                    print(f'Saving temprary file to {tmp_file}')

                    # Compress the data with built-in zlib method
                    comp = dict(zlib=True, complevel=2, fletcher32=True)
                    
                    for var in ds: 
                        ds[var].encoding.update(comp)
                    
                    ds.to_zarr(tmp_file)
                    print(f'{tmp_file} was saved.')
                    ds.close()

                    if len(args.algorithm) == 0:
                        return
                    else:
                        with xarray.open_dataset(tmp_file, engine='zarr') as Dataset:
                            ds = Dataset.load()
                else:
                    ds_memory = ds.compute()
                    ds.close()
                    ds = ds_memory

    # Apply algorithm if specified
    if len(args.algorithm) > 0:
        for _i, alg_input in enumerate(args.algorithm):
            try:
                assert alg_input != 'rio_decorator'
                alg = getattr(algs, alg_input)
                print(f'Calculate {alg_input} of the data.')
                ds = alg(ds)

            except AttributeError:
                print(f'Algorithm {alg_input} does not exist in algs module. Raw data will be exported')
                args.algorithm[_i] = None

            except AssertionError:
                print(f'Algorithm {alg_input} does not exist in algs module. Raw data will be exported')
                args.algorithm[_i] = None
        
        # Save spatial information using CF convention
        ds = update_dtype(ds, args)
        ds = save_crs(ds)

        if tmp_dir:
            tmp_file = Path(tmp_dir) / f"{feature_id}_{'_'.join([str(pp) for pp in geom_geobox.boundingbox])}.zarr"
            print(f'Saving temprary file to {tmp_file}')

            # Compress the data with built-in zlib method
            comp = dict(zlib=True, complevel=2, fletcher32=True)
            
            for var in ds: 
                ds[var].encoding.update(comp)
            
            ds.to_zarr(tmp_file, mode='w')
            print(f'{tmp_file} was saved.')
            ds.close()

            return

    return ds


def export_data(ds, output_bands, args, out_file, feature_id=None, tile_number=(None, None)):
    # Make sure the output bands only contains available variables
    output_bands = [_band for _band in output_bands if _band in ds]

    # Check if exporting image is needed
    if args.noexport:
        print('Return the Dataset as an object in memory')
        if feature_id is None:
            return ds[output_bands].compute()
        else:
            return (feature_id, ds[output_bands].compute())
    
    else:
        # Compute the data
        print('Computing the final dataset...')
        ds.persist()

        # Create the directory if it does not exist
        if not args.out_dir.exists():
            args.out_dir.mkdir(parents=True)

        # Check if the data is multi-temporal
        if 'time' in ds.dims.keys():
            # Create a sub-directory to store the multi-temporal images
            sub_dir = args.out_dir / out_file
            if not sub_dir.exists():
                sub_dir.mkdir(parents=True)
            
            if args.out_ext == 'tif':
                ds = ds.dropna(dim='time', how='all')
                for time_step in range(ds.dims['time']):
                    date = ds.isel(time=time_step).time.dt.strftime('%Y-%m-%d').data
                    suffix = '_'.join([str(e) for e in [*args.algorithm, date, tile_number[0], tile_number[1]] if e is not None])
                    if suffix != '':
                        output_file = (sub_dir / f'{out_file}_{suffix}.tif').as_posix()
                    else:
                        output_file = (sub_dir / f'{out_file}.tif').as_posix()
                    print(f"Cloud cover on {date} within regions of interest: {ds['cloudcover'].isel(time=time_step).values:.2f}%")
                    if ds['cloudcover'].isel(time=time_step) <= args.cloudcover:
                        print(f'Saving image to {output_file}')
                        ds[output_bands[:-1]].isel(time=time_step).squeeze().rio.to_raster(output_file, driver='COG')
                        print(f'{output_file} was saved.')
                    else:
                        print(f'Image on {date} has too many clouds.')
            else:
                # Save cloudcover as a variable
                ds = ds.where(ds['cloudcover'] <= args.cloudcover).dropna(dim='time', how='all')
                # Compress the data with built-in zlib method
                comp = dict(zlib=True, complevel=2, fletcher32=True)
                
                suffix = '_'.join([str(e) for e in [*args.algorithm, tile_number[0], tile_number[1]] if e is not None])
                if suffix != '':
                    output_file = (sub_dir / f'{out_file}_{suffix}.nc').as_posix()
                else:
                    output_file = (sub_dir / f'{out_file}.nc').as_posix()
                print(f'Saving image to {output_file}')
                output_ds = update_dtype(ds[output_bands], args)
                for var in output_ds: 
                    output_ds[var].encoding.update(comp)
                output_ds.to_netcdf(output_file)
                print(f'{output_file} was saved.')

        else:
            suffix = '_'.join([str(e) for e in [*args.algorithm, tile_number[0], tile_number[1]] if e is not None])
            if suffix != '':
                output_file = (args.out_dir / f'{out_file}_{suffix}.{args.out_ext}').as_posix()
            else:
                output_file = (args.out_dir / f'{out_file}.{args.out_ext}').as_posix()
            try:
                print(f"Cloud cover within regions of interest: {ds['cloudcover'].values:.2f}%")
            except TypeError:
                print(f"Cloud cover within regions of interest: {ds['cloudcover'].values[0]:.2f}%")
            if ds['cloudcover'] <= args.cloudcover:
                print(f'Saving image to {output_file}')
                if args.out_ext == 'tif':
                    ds[output_bands[:-1]].squeeze().rio.to_raster(output_file, driver='COG')
                else:
                    # Compress the data with built-in zlib method
                    comp = dict(zlib=True, complevel=2, fletcher32=True)
                    
                    output_ds = update_dtype(ds[output_bands], args)
                    for var in output_ds: 
                        output_ds[var].encoding.update(comp)
                    output_ds.to_netcdf(output_file)
                print(f'{output_file} was saved.')
            else:
                print('Image has too many clouds.')
        return
        

def export_data_tiles(ds, output_bands, args, out_file, feature_id):
    spatial_dims = ds.odc.spatial_dims
    result = list()
    for x, y in [(x, y) for x in range(0, ds.dims[spatial_dims[1]], int(args.tile_size*(1-args.overlap/100.0))) \
                    for y in range(0, ds.dims[spatial_dims[0]], int(args.tile_size*(1-args.overlap/100.0)))]:
        ds_current = ds.isel({spatial_dims[1]: slice(x, x+args.tile_size), spatial_dims[0]: slice(y, y+args.tile_size)})
        if not ds_current.dataMask.where(ds_current.dataMask==1).isnull().all():
            _result = export_data(ds_current, output_bands, args, out_file, feature_id, tile_number=(x, y))
            if _result:
                result.append(_result)
    # Return None if result is empty
    if len(result) == 0:
        return
    else:
        return result


# Define a helper function for tiling geobox
def process_tile(bbox, geom):
    bbox_geom = bbox.footprint('EPSG:4326')
    geom_intersection = geom.intersection(bbox_geom)
    return bbox, geom_intersection


def find_appropriate_crs(geom):
    utm_grid_file = Path(__file__).parent / 'World_UTM_Grid.zip'
    utm_zones = gpd.read_file(utm_grid_file)
    geom_utms = gpd.overlay(gpd.GeoDataFrame(geometry=[shapely.from_wkt(geom.wkt)], crs='EPSG:4326'), utm_zones, how='intersection')
    # Set UTM zones for North hemisphere
    geom_utms['EPSG'] = 32600 + geom_utms['ZONE']
    # Check if it's in south hemisphere
    geom_utms.loc[geom_utms['ROW_'] <= 'M', 'EPSG'] = 32700 + geom_utms.loc[geom_utms['ROW_'] <= 'M', 'ZONE']
    if len(geom_utms['EPSG'].unique()) == 1:
        crs = f"EPSG:{geom_utms['EPSG'].unique()[0]}"
    # If the geometry cross multiple UTM zones, the operation is in EPSG:9473 (GDA2020 / Australian Albers) in order to get homogeneous pixel resolution
    else:
        crs = 'EPSG:9473'
    return crs


def save_crs(ds):
    # Save the spatial information following CF convention standard
    # Create the CRS coordinate with the necessary attributes
    spatial_dims = ds.odc.spatial_dims
    ds = ds.rio.set_spatial_dims(x_dim=spatial_dims[1], y_dim=spatial_dims[0])
    ds = ds.rio.write_coordinate_system()
    
    crs = CRS(ds.rio.crs.to_string())
    crs_attrs = {
        'grid_mapping_name': crs.coordinate_system.name if crs.coordinate_system else 'unknown',
        'epsg_code': crs.to_epsg(),
        'spatial_ref': crs.to_wkt()
    }

    # Additional attributes based on CRS information
    if crs.is_geographic:
        crs_attrs['semi_major_axis'] = crs.ellipsoid.semi_major_metre if crs.ellipsoid else None
        crs_attrs['inverse_flattening'] = crs.ellipsoid.inverse_flattening if crs.ellipsoid else None
    elif crs.is_projected:
        # Add attributes relevant to projected CRS
        crs_attrs['proj_name'] = crs.to_dict().get('proj')
        datum = crs.to_dict().get('datum')
        if not datum:
            datum = 'GDA2020'
        crs_attrs['datum'] = datum
        crs_attrs['units'] = crs.to_dict().get('units')

    # Create the crs coordinate
    crs_coord = xarray.DataArray(0, name='crs', attrs=crs_attrs)

    # Add the crs coordinate to the dataset
    ds = ds.assign_coords(crs=crs_coord)
    for _var in ds:
        ds[_var].attrs['grid_mapping'] = 'crs'
        try:
            del ds[_var].encoding['grid_mapping']
        except KeyError:
            pass
    
    return ds


def update_dtype(ds, args):
    for var in ds:
        if var in ['cloudcover', 'geom']:
            ds[var] = ds[var].astype(np.int8)
        elif var in args.algorithm:
            ds[var] = ds[var].astype(np.float32)
        else:
            ds[var] = ds[var].astype(np.int16)
        ds[var].encoding = {'dtype': ds[var].dtype}
    return ds


def sanitise_filename(name: str | None) -> str:
    if name is None:
        return "unnamed"
    """
    Convert a string to a filename-safe string for Windows and Linux.
    """
    name = name.strip()

    # Replace anything except letters, numbers, _, -, and . with _
    name = re.sub(r"[^\w.-]+", "_", name)

    # Collapse consecutive underscores
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing dots and underscores
    name = name.strip("._")

    # Windows reserved device names
    reserved = {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }

    if name.upper() in reserved:
        name = f"_{name}"

    return name or "unnamed"


# A function to retry
def retry(times):

    def wrapper_fn(f):
        @functools.wraps(f)
        def new_wrapper(*args,**kwargs):
            for i in range(times):
                try:
                    return f(*args,**kwargs)
                except rasterio.errors.RasterioIOError as e:
                    error = e
                    print(error)
                    print('Retry after 10 seconds')
                    if i == times -2:
                        kwargs['fail_on_error'] = False
                    time.sleep(10)
            print(f'Retried {times} times. Stopping processing.')
            raise error
        return new_wrapper

    return wrapper_fn


# Maximum retry 5 times
@retry(5)
def workflow(args, geom, selected_bands, feature_id=None, fail_on_error=True):
    # Preserve the initial output filename
    out_file = args.out_file
    # Check if the input geom has name
    if isinstance(geom, tuple):
        feature_id = geom[0]
        out_file = f'{args.out_file}_{feature_id}'
        geom = geom[1]

    # Find the appropriate CRS for the input geometry
    crs = find_appropriate_crs(geom)
    
    # Check the size of the geometry
    # Split it into tiles if it is larger than 2000 x 2000 pixels
    geom_geobox_proj = geobox.GeoBox.from_geopolygon(geom, resolution=args.resolution, crs=crs)
    geom_geobox_tiles = geobox.GeoboxTiles(geom_geobox_proj, (size_limit, size_limit))

    # Create a temporary directory to store tile images
    with tempfile.TemporaryDirectory(dir=args.tmp_dir) as tiles_dir:
        if geom_geobox_tiles.shape.x == 1 and geom_geobox_tiles.shape.y == 1:
            ds_proj = get_dataset(args, geom_geobox_proj, geom, selected_bands, feature_id, tiles_dir, fail_on_error=fail_on_error)
            
            if ds_proj is None:
                # Delayed loading the dataset with dask chunks
                try:
                    ds_proj = xarray.open_mfdataset(Path(tiles_dir).glob(f'{feature_id}_*.zarr'), 
                                                    engine='zarr', parallel=True)
                    
                except OSError:
                    print('No dataset was found.')
                    return
            
        else:
            print('The extent of input geometry is too large. Query the data in tiles.')
            # Calculate the geobox for tiles
            tile_x = geom_geobox_tiles.shape.x
            tile_y = geom_geobox_tiles.shape.y
            geobox_list = list()
            geom_list = list()
            
            with concurrent.futures.ProcessPoolExecutor() as executor:
                # Use list comprehension to submit tasks to the thread pool
                futures = [executor.submit(process_tile, geom_geobox_tiles[_y, _x], geom) for _x, _y in [(x, y) for x in range(tile_x) for y in range(tile_y)]]

                # Retrieve results as they become available
                for future in concurrent.futures.as_completed(futures):
                    bbox, geom_intersection = future.result()
                    geobox_list.append(bbox)
                    geom_list.append(geom_intersection)

            print(f'The input extent is splited into {len(geom_list)} tiles.')
            # Request data for every tile. Maximum thread number set to task limit to avoid spamming the server
            with concurrent.futures.ThreadPoolExecutor(max_workers=TASK_LIMIT) as executor:
                results = executor.map(get_dataset, 
                                       [args]*len(geom_list), 
                                       geobox_list, geom_list, 
                                       [selected_bands]*len(geom_list), 
                                       [feature_id]*len(geom_list), 
                                       [tiles_dir]*len(geom_list), 
                                       [fail_on_error]*len(geom_list))

                # Make sure all the tasks finish properly
                for r in results:
                    pass
            
            print('Merging datasets...')
            
            # Delayed loading the dataset with dask chunks
            try:
                ds_proj = xarray.open_mfdataset(Path(tiles_dir).glob(f'{feature_id}_*.zarr'), 
                                                engine='zarr', parallel=True)
            except OSError:
                print('No dataset was found.')
                return
        
        # Calculate cloud cover percentage
        cc = (1 - (ds_proj.dataMask.sum(dim=['x', 'y']) / ds_proj.geom.sum(dim=['x', 'y']))) * 100
        
        # Assign cloud cover as a coordinate
        cloudcover = cc.compute().astype(np.int8)
        ds_proj = ds_proj.assign_coords(cloudcover=cloudcover)

        # Remove dtype encoding if there is any
        ds_proj = update_dtype(ds_proj, args)

        # Save the spatial information using CF convention
        ds_proj = save_crs(ds_proj)

        # Assign the boundary WKT as attribute
        ds_proj.attrs['boundary'] = geom.wkt
        
        if args.tile_size is None:
            result = export_data(ds_proj, selected_bands[1], args, out_file, feature_id)
            
        else:
            result = export_data_tiles(ds_proj, selected_bands[1], args, out_file, feature_id)
        ds_proj.close()
    
    return result


def main(args):
    # Create a dask client
    with dask.distributed.Client(silence_logs=logging.ERROR) as client:

        # odc-stac library downloads DEA datasets stored in AWS
        # when external to AWS (like outside DEA sandbox), AWS signed requests must be disabled
        odc.stac.configure_rio(cloud_defaults=True, aws={"aws_unsigned": True}, client=client)

        # Set the collections to Geoscience Australia Landsat-8, Landsat-9, and Sentinel-2 ARD collection 3 if not specified
        if args.collection is None:
            args.collection = ['ga_ls8c_ard_3', 'ga_ls9c_ard_3', 'ga_s2am_ard_3', 'ga_s2bm_ard_3', 'ga_s2cm_ard_3']
        elif len(args.collection) == 1:
            if args.collection[0].lower() == 'landsat':
                args.collection = ['ga_ls8c_ard_3', 'ga_ls9c_ard_3']
            if args.collection[0].lower() == 'sentinel':
                args.collection = ['ga_s2am_ard_3', 'ga_s2bm_ard_3', 'ga_s2cm_ard_3']

        # Set the bands
        if args.spectral_band == 'rgb':
            selected_bands = variables.rgb_bands
        elif args.spectral_band == 'bgrn':
            selected_bands = variables.bgrn_bands
        elif args.spectral_band == 'common':
            selected_bands = variables.common_bands
        elif args.spectral_band == 'all':
            selected_bands = variables.all_bands
        else:
            parsed_bands = args.spectral_band.split(',')
            selected_bands = [{'landsat': [*parsed_bands ,*[_band for _band in variables.cloud_mask_bands_ls if _band not in parsed_bands]], 
                               'sentinel': [*parsed_bands, *[_band for _band in variables.cloud_mask_bands_s2 if _band not in parsed_bands]]}, 
                               parsed_bands + ['dataMask', 'cloudcover']]
            
        # Set the band based on the algorithm
        if 'ndvi' in args.algorithm:
            selected_bands = [{'landsat': variables.query_bands_bgrn_ls, 'sentinel': variables.query_bands_bgrn_s2}, 
                              ['ndvi', 'dataMask', 'cloudcover']]

        # Parse the input geometry
        if args.vector is not None:
            gdf = gpd.read_file(args.vector)
            # Make sure the CRS is EPSG:4326
            gdf_wgs84 = gdf.to_crs('EPSG:4326')
            if args.union:
                geom = gdf_wgs84.union_all()
                # Drop Z dimension if there is any
                geom = shapely.force_2d(geom)
                # Assign CRS
                geom = odc.geo.geom.Geometry(geom, crs='EPSG:4326')
            else:
                # Add every feature to the list
                geom = list()
                for feature in gdf_wgs84.itertuples():
                    if args.fid_column is not None and args.fid_column in gdf_wgs84.columns:
                        feature_id = getattr(feature, args.fid_column)
                        # Make the feature_id filename safe
                        feature_id = sanitise_filename(str(feature_id))
                    else:
                        feature_id = feature.Index
                    feature_geom = feature.geometry
                    # Drop Z dimension if there is any
                    feature_geom = shapely.force_2d(feature_geom)
                    # Assign CRS
                    feature_geom = odc.geo.geom.Geometry(feature_geom, crs='EPSG:4326')
                    geom.append((feature_id, feature_geom))
            
        elif args.bbox is not None:
            # Create geometry based on the input bounding box
            geom = shapely.geometry.box(*[float(pp) for pp in args.bbox.split(',')])
            # Assign CRS
            geom = odc.geo.geom.Geometry(geom, crs='EPSG:4326')
        else:
            # Create geometry for the whole Australia if both --vector and --bbox are not specified
            print('Warning! No --vector and --bbox are specified. Retrieving data for the whole Australia.')
            geom = country_geom('AUS', crs='EPSG:4326')

        # Parse the tile overlap
        if args.tile_size is not None and args.overlap is None:
            args.overlap = 0

        # Process every features if there are many
        if isinstance(geom, list):
            print(f'Found {len(geom)} features.')
            result = list()
            with concurrent.futures.ThreadPoolExecutor(max_workers=TASK_LIMIT) as executor:
                futures = [executor.submit(workflow, args, _geom, selected_bands) for _geom in geom]
                for future in concurrent.futures.as_completed(futures):
                    ds = future.result()
                    result.append(ds)
        else:
            result = workflow(args, geom, selected_bands)
        
        # Return the Dataset if --noexport is specified
        # Otherwise, return None
        return result


if __name__ == '__main__':
    nl = '\n'
    parser = argparse.ArgumentParser(description="Access Digital Earth Australia products via STAC API.", 
                                     formatter_class=RawTextHelpFormatter)

    # Argument for the vector file of the area of interest
    parser.add_argument('-v', '--vector', type=str, 
                        help="Path to the vector file of area of interest. Cannot be used with --bbox\n" \
                             "If --vector and --bbox are not specified, read the data for whole Australia.", 
                        action=VerifyNoBbox)
    
    # Argument for vector union operation
    parser.add_argument('--union', action='store_true', 
                        help="By default, the program will loop through every geometry of the input vector file to retrieve data. \n" \
                             "If --union is specified, create union geometry of the input vector.\n" \
                             "If it is not used with --vector, this argument will be ignored.")

    # Argument for the bounding box of the area of interest
    parser.add_argument('-b', '--bbox', type=str, 
                        help="Bounding box in the format of <lon_min>,<lat_min>,<lon_max>,<lat_max>. Cannot be used with --vector\n" \
                             "If --vector and --bbox are not specified, read the data for whole Australia.", 
                        action=VerifyNoVector)
    
    # Argument for the STAC collections
    parser.add_argument('-c', '--collection', type=str, 
                        help="Name of STAC collection (default to Geoscience Australia Landsat-8, Landsat-9, and Sentinel-2 ARD collection 3). \n"\
                             'You can also specify "sentinel" or "landsat" to request data from specified constellation. \n'
                             "Otherwise, specify the collection name (e.g. ga_s2am_ard_3). In this case, it can be specified multiple times", 
                        action='append')
    
    # Argument for spatial resolution
    parser.add_argument('-r', '--resolution', default=10, type=float, 
                        help="Spatial resolution of the data in metre. Default to 10m.")

    # Argument for output bands
    parser.add_argument('-sb', '--spectral_band', type=str, 
                        help="Requested bands. Default to Landsat-Sentinel common bands + dataMask. \n"\
                             "The following options are accepted: \n"\
                             f"{nl.join(['* rgb', '* bgrn', '* common', '* all', '* <band a>,<band b>,<band c>,...,<band n>'])}", 
                        default='common')
    
    # Argument for cloud cover
    parser.add_argument('-cc', '--cloudcover', type=int, 
                        help="The acceptable cloud cover threshold. The value should be between 0 and 100. Default to 100 if not specified", 
                        default=100, choices=range(101), metavar='0-100')
    
    # Argument for the algorithm
    parser.add_argument('-a', '--algorithm', type=str, 
                        help="Algorithm of how the data will be processed. If not specified, the raw data will be exported. \n"\
                             "Can be specified multiple times, and the algorithms will be executed based on the input order\n"\
                             "The following algorithms are available: \n"\
                             f"{nl.join(['* '+f for f, _ in inspect.getmembers(algs, inspect.isfunction) if f != 'rio_decorator'])}", 
                        action='append', default=list())
    
    # Argument for export images
    parser.add_argument('--noexport', action='store_true', 
                        help="If specified, instead of saving the data to image files, it returns the Dataset as an object in memory. \n"\
                              "Could be useful when combining with other pipelines.")
    
    # Argument for output directory
    parser.add_argument('-od', '--out_dir', type=Path, 
                        help="Output directory. Default to user home directory.", 
                        default=Path.home())
    
    # Argument for output file basename
    parser.add_argument('-of', '--out_file', type=str, 
                        help="The basename of the output images. Default to 'output'", 
                        default='output')

    # Argument for output feature ID
    parser.add_argument('-fc', '--fid_column', type=str, 
                        help="When union is not specified, the default output filename is <out_file>_<fid>. \n"\
                             "If fid_column is specified it will try to use the value from that column to replace fid", 
                        default=None)
    
    # Argument for output file basename
    parser.add_argument('-ox', '--out_ext', type=str, 
                        help="The format of the output images (either tif or nc). Default to 'tif'", 
                        default='tif', choices=['tif', 'nc'])
    
    # Argument for the directory that store temporary NetCDF files
    parser.add_argument('-td', '--tmp_dir', type=Path, 
                        help="Temporary directory that store temporary NetCDF files. \n"\
                             "If not specified, it will create a temporary folder under the system temporary directory if necessary.")
    
    # Argument for output tile size
    parser.add_argument('-t', '--tile_size', type=int, 
                        help="The tile size of output images. The value should be >= 5. \n"\
                             "If not specified, the images will be exported as a whole.", 
                        action=VerifyTilesize)
    
    # Argument for output tile overlap percentage
    parser.add_argument('-ol', '--overlap', type=int, 
                        help="The overlap percentage of the output tiles. The value should be between 0 and 80. \n"\
                             "Should be used with --tile_size.", 
                        action=VerifyOverlap)
    
    # Argument for the start date
    parser.add_argument('date_start', metavar='d1', type=str, 
                        help="Start date in YYYY-MM-DD format.")
    
    # Argument for the end date
    parser.add_argument('date_end', metavar='d2', type=str, 
                        help="End date in YYYY-MM-DD format.")
    
    args = parser.parse_args()

    main(args)
