import warnings
warnings.filterwarnings("ignore")

from html.parser import HTMLParser
import time
import json
import concurrent.futures
from pathlib import Path

import pystac_client
from odc.geo import xr
import odc.geo.geom
import xarray
import shapely
import rasterio
import rioxarray as rio
import geopandas as gpd
from pyproj import CRS

try:
    from . import utils
except ImportError:
    from DEA import utils


dea_stac_url = 'https://explorer.dea.ga.gov.au/stac'
# collections = ['ls8_barest_earth_albers']
collections = ["landsat_barest_earth"] # V2.1.2
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


def get_stac_pages(collections, geom):
    while True:
        try: 
            # Sleep for 200 milliseconds to avoid abusing web server
            time.sleep(0.2)

            catalog = pystac_client.Client.open(dea_stac_url)

            # Build a query with the set parameters
            query = catalog.search(max_items=None, 
                                   limit=100, 
                                   intersects=geom, 
                                   collections=collections)

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


def load_as_datacube(query_items, bbox):
    # All operation is in native CRS (3577)
    ds_list = list()
    for query_item in query_items:
        da_dict = dict()
        for band, asset in query_item.assets.items():
            with rio.open_rasterio(asset.href) as src:
                da_dict[band] = src.rio.clip_box(*bbox).squeeze(dim="band").load()
        _ds = xarray.Dataset(da_dict)
        ds_list.append(_ds)

    if len(ds_list) > 1:
        ds = xarray.merge(ds_list, join="outer", compat="no_conflicts")
    else:
        ds = ds_list[0]
    
    return ds


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


def export_data(ds, out_dir, out_file):
    # Compute the data
    print('Computing the final dataset...')
    ds.persist()
    output_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']

    # Create the directory if it does not exist
    if not Path(out_dir).exists():
        Path(out_dir).mkdir(parents=True)

    output_file = (Path(out_dir) / f'{out_file}.tif').as_posix()

    print(f'Saving image to {output_file}')
    ds[output_bands].squeeze().rio.to_raster(output_file, driver='COG')

    print(f'{output_file} was saved.')

    return


def workflow(collections, geom, out_dir, out_file):
    # crs = find_appropriate_crs(geom)
    crs = 3577
    bbox_3577 = geom.to_crs(crs).boundingbox

    pages = get_stac_pages(collections, geom)
    items = get_stac_items(pages)
    ds = load_as_datacube(items, bbox_3577)
    ds = save_crs(ds)
    export_data(ds, out_dir, out_file)
    return


def main(vector, out_dir, out_file, union=False, fid_column=None, collections=collections):
    # odc-stac library downloads DEA datasets stored in AWS
    # when external to AWS (like outside DEA sandbox), AWS signed requests must be disabled
    with rasterio.env.Env(aws_unsigned=True):
        # Parse the input geometry
        gdf = gpd.read_file(vector)
        # Make sure the CRS is EPSG:4326
        gdf_wgs84 = gdf.to_crs('EPSG:4326')
        if union:
            geom = gdf_wgs84.union_all()
            # Drop Z dimension if there is any
            geom = shapely.force_2d(geom)
            # Assign CRS
            geom = odc.geo.geom.Geometry(geom, crs='EPSG:4326')
        else:
            # Add every feature to the list
            geom = list()
            for feature in gdf_wgs84.itertuples():
                if fid_column is not None and fid_column in gdf_wgs84.columns:
                    feature_id = getattr(feature, fid_column)
                    # Make the feature_id filename safe
                    feature_id = utils.sanitise_filename(str(feature_id))
                else:
                    feature_id = feature.Index
                feature_geom = feature.geometry
                # Drop Z dimension if there is any
                feature_geom = shapely.force_2d(feature_geom)
                # Assign CRS
                feature_geom = odc.geo.geom.Geometry(feature_geom, crs='EPSG:4326')
                geom.append((feature_id, feature_geom))

        # Process every features if there are many
        if isinstance(geom, list):
            print(f'Found {len(geom)} features.')
            result = list()
            with concurrent.futures.ThreadPoolExecutor(max_workers=TASK_LIMIT) as executor:
                futures = [executor.submit(workflow, _geom, out_dir, out_file) for _geom in geom]
                for future in concurrent.futures.as_completed(futures):
                    ds = future.result()
                    result.append(ds)
        else:
            result = workflow(collections, geom, out_dir, out_file)
        
        return result
    

if __name__ == '__main__':
    import argparse
    from argparse import RawTextHelpFormatter
    nl = '\n'
    parser = argparse.ArgumentParser(description="Download the Landsat Barest Earth data through Digital Earth Australia.", 
                                     formatter_class=RawTextHelpFormatter)

    # Argument for the vector file of the area of interest
    parser.add_argument('vector', type=str, 
                        help="Path to the vector file of area of interest.")

    # Argument for output directory
    parser.add_argument('out_dir', type=Path, 
                        help="Output directory. Default to user home directory.", 
                        default=Path.home())
    
    # Argument for output file basename
    parser.add_argument('out_file', type=str, 
                        help="The basename of the output images. Default to 'output'", 
                        default='output')
    
    # Argument for vector union operation
    parser.add_argument('--union', action='store_true', 
                        help="By default, the program will loop through every geometry of the input vector file to retrieve data. \n" \
                             "If --union is specified, create union geometry of the input vector.")

    # Argument for output feature ID
    parser.add_argument('-fc', '--fid_column', type=str, 
                        help="When union is not specified, the default output filename is <out_file>_<fid>. \n"\
                             "If fid_column is specified it will try to use the value from that column to replace fid", 
                        default=None)

    args = parser.parse_args()
    
    main(args.vector, args.out_dir, args.out_file, args.union, args.fid_column)