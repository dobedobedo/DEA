# Default bands combinations
cloud_mask_bands_s2 = ['nbart_green', 'nbart_red', 'nbart_swir_2', 'oa_s2cloudless_prob', 'oa_fmask', 'oa_solar_azimuth']
cloud_mask_bands_ls = ['nbart_green', 'nbart_red', 'nbart_swir_1', 'oa_fmask', 'oa_solar_azimuth']

query_bands_common_s2 = ['nbart_blue', 'nbart_green', 'nbart_red', 
                         'nbart_nir_1', 'nbart_swir_2', 'nbart_swir_3', 
                         'oa_s2cloudless_prob', 'oa_fmask', 'oa_solar_azimuth']
query_bands_common_ls = ['nbart_blue', 'nbart_green', 'nbart_red', 
                         'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'oa_fmask', 'oa_solar_azimuth']


query_bands_all_s2 = ['nbart_blue', 'nbart_green', 'nbart_red', 
                      'nbart_red_edge_1', 'nbart_red_edge_2', 'nbart_red_edge_3', 
                      'nbart_nir_1', 'nbart_nir_2', 'nbart_swir_2', 'nbart_swir_3', 
                      'oa_s2cloudless_prob', 'oa_fmask', 'oa_solar_azimuth']
query_bands_all_ls = ['nbart_blue', 'nbart_green', 'nbart_red', 
                      'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'oa_fmask', 'oa_solar_azimuth']

query_bands_rgb_s2 = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir_1', 'nbart_swir_2', 'oa_s2cloudless_prob', 'oa_fmask', 'oa_solar_azimuth']
query_bands_rgb_ls = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'oa_fmask', 'oa_solar_azimuth']

query_bands_bgrn_s2 = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir_1', 'nbart_swir_2','oa_s2cloudless_prob', 'oa_fmask', 'oa_solar_azimuth']
query_bands_bgrn_ls = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'oa_fmask', 'oa_solar_azimuth']

lsband_rename_table = {'nbart_nir': 'nbart_nir_1', 'nbart_swir_1': 'nbart_swir_2', 'nbart_swir_2': 'nbart_swir_3'}

output_bands_common = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir_1', 
                       'nbart_swir_2', 'nbart_swir_3', 'dataMask', 'cloudcover']
output_bands_all = ['nbart_blue', 'nbart_green', 'nbart_red', 
                    'nbart_red_edge_1', 'nbart_red_edge_2', 'nbart_red_edge_3', 
                    'nbart_nir_1', 'nbart_nir_2', 'nbart_swir_2', 'nbart_swir_3', 'dataMask', 'cloudcover']
output_bands_rgb = ['nbart_red', 'nbart_green', 'nbart_blue', 'dataMask', 'cloudcover']
output_bands_bgrn = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir_1', 'dataMask', 'cloudcover']

sunglint_bands = ['oa_solar_zenith', 'oa_solar_azimuth', 'oa_satellite_azimuth', 'oa_satellite_view']

common_bands = [{'landsat': query_bands_common_ls, 'sentinel': query_bands_common_s2}, output_bands_common]
all_bands = [{'landsat': query_bands_all_ls, 'sentinel': query_bands_all_s2}, output_bands_all]
rgb_bands = [{'landsat': query_bands_rgb_ls, 'sentinel': query_bands_rgb_s2}, output_bands_rgb]
bgrn_bands = [{'landsat': query_bands_bgrn_ls, 'sentinel': query_bands_bgrn_s2}, output_bands_bgrn]
