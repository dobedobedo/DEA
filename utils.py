from pathlib import Path
import re
from html.parser import HTMLParser
import argparse

import geopandas as gpd
import shapely
from pyproj import CRS
from odc.geo import xr
import xarray
import numpy as np


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