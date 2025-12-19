## A wrapper to download NBART corrected surface reflectance of Landsat-8-9 and Sentinel-2 satellite images from Digital Earth Australia  
### This is a CLI tool, please use `python -m DEA.dea_stac --help` to see more options:  

usage: dea_stac.py [-h] [-v VECTOR] [--union] [-b BBOX] [-c COLLECTION] [-r RESOLUTION] [-sb SPECTRAL_BAND] [-cc 0-100] [-a ALGORITHM]
                   [--noexport] [-od OUT_DIR] [-of OUT_FILE] [-ox {tif,nc}] [-td TMP_DIR] [-t TILE_SIZE] [-ol OVERLAP]
                   d1 d2  

Examples:  
`python -m DEA.dea_stac -v \<vector-file-path\> -od \<output_directory\> -ox nc 2017-01-01 2020-12-31`  
  
The above command will download the common bands of Landsat-Sentinel time series data between 2017-01-01 and 2020-12-31 within the extents of every feature in the specified vector file in netCDF4 format and save it to the specified output directory. The default output files are output_\<FID\>.nc

`python -m DEA.dea_stac -b \<min lon\>,\<min lat\>,\<max lon\>,\<max lat\> -c sentinel -sb all -cc 20 -ox tif 2017-01-01 2020-12-31`  

The above command will download all bands of Sentinel-2 time series data between 2017-01-01 and 2020-12-31 within the specified bounding box. It will discard any images where the calculated cloud cover is more than 20%, and save the result of each timestamp as a Geotiff file under users' home folder as output_\<date\>.tif  
