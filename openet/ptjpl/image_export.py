import os
import sys
import time
from datetime import datetime

import ee
import geopandas as gpd
import openet.core
from tqdm import tqdm

from openet.ptjpl.ee_utils import landsat_masked, is_authorized

sys.path.insert(0, os.path.abspath('../..'))
sys.setrecursionlimit(5000)

EC_POINTS = 'users/dgketchum/fields/flux'

STATES = ['AZ', 'CA', 'CO', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WA', 'WY']
WEST_STATES = 'users/dgketchum/boundaries/western_11_union'
EAST_STATES = 'users/dgketchum/boundaries/eastern_38_dissolved'


def export_et_fraction(shapefile, asset_root, check_dir=None, grid_spec=None, feature_id='FID', select=None,
                       start_yr=2000, end_yr=2024):
    df = gpd.read_file(shapefile)
    df.index = df[feature_id]

    assert df.crs.srs == 'EPSG:5071'

    df = df.to_crs(epsg=4326)

    skipped, exported = 0, 0

    for fid, row in tqdm(df.iterrows(), desc='Export PTJPL', total=df.shape[0]):

        for year in range(start_yr, end_yr + 1):

            if select is not None and fid not in select:
                continue

            site = row[feature_id]
            grid_sz = row['grid_size']

            if grid_spec is not None and grid_sz != grid_spec:
                continue

            polygon = ee.Geometry.Polygon([[c[0], c[1]] for c in row['geometry'].exterior.coords])
            fc = ee.FeatureCollection(ee.Feature(polygon, {feature_id: site}))

            coll = landsat_masked(year, fc)
            scenes = coll.aggregate_histogram('system:index').getInfo()

            for img_id in scenes:
                splt = img_id.split('_')
                _name = '_'.join(splt[-3:])
                inst = splt[-3]
                tile = splt[-2]
                p, r = int(tile[:3]), int(tile[-3:])

                landsat_img = ee.Image(f'LANDSAT/{inst}/C02/T1_L2/{_name}')
                info = landsat_img.getInfo()
                proj = landsat_img.select('SR_B1').getInfo()['bands'][0]
                dims = proj['dimensions']
                crs = proj['crs']
                crs_transform = proj['crs_transform']
                dims = f'{dims[0]}x{dims[1]}'
                model_package_name = 'ptjpl'

                export_id = f'{model_package_name}_{info['id']}'

                etf = openet.ptjpl.Image.from_landsat_c2_sr(landsat_img,
                                                            et_reference_source='IDAHO_EPSCOR/GRIDMET',
                                                            et_reference_band='eto',
                                                            et_reference_factor=1.0,
                                                            et_reference_resample='bilinear',
                                                            ).et_fraction

                properties = {
                    'build_date': datetime.today().strftime('%Y-%m-%d'),
                    'coll_id': os.path.dirname(info['id']),
                    'core_version': openet.core.__version__,
                    'image_id': info['id'],
                    'model_name': 'openet-ptjpl',
                    'model_version': "0.4.1",
                    'scale_factor': 1.0 / 10000,
                    'scene_id': _name,
                    'wrs2_path': p,
                    'wrs2_row': r,
                    'wrs2_tile': tile,
                    'CLOUD_COVER': info['properties']['CLOUD_COVER'],
                    'CLOUD_COVER_LAND': info['properties']['CLOUD_COVER_LAND'],
                    'system:index': _name,
                    'system:time_start': info['properties']['system:time_start'],
                }

                etf.set(properties)

                export_path = os.path.join(asset_root, _name)

                task = ee.batch.Export.image.toAsset(
                    etf,
                    description=_name,
                    assetId=export_path,
                    dimensions=dims,
                    crs=crs,
                    crsTransform=crs_transform,
                    maxPixels=1e13)

                try:
                    task.start()
                    print(_name)
                    exit()
                except ee.ee_exception.EEException as e:
                    print('{}, waiting on '.format(e), _name, '......')
                    time.sleep(600)
                    task.start()
                exported += 1


if __name__ == '__main__':

    is_authorized()

    bucket = 'wudr'

    home = os.path.expanduser('~')
    root = os.path.join(home, 'PycharmProjects', 'swim-rs')
    shapefile_path = os.path.join(root, 'footprints', 'flux_static_footprints.shp')

    data = os.path.join(root, 'tutorials', '4_Flux_Network', 'data')
    landsat_dst = os.path.join(data, 'landsat')

    fields_gridmet = os.path.join(data, 'gis', 'flux_fields_gfid.shp')

    fdf = gpd.read_file(fields_gridmet)
    target_states = ['AZ', 'CA', 'CO', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WA', 'WY']
    state_idx = [i for i, r in fdf.iterrows() if r['field_3'] in target_states]
    fdf = fdf.loc[state_idx]
    sites_ = list(set(fdf['field_1'].to_list()))
    sites_.sort()

    # Volk static footprints
    FEATURE_ID = 'site_id'
    state_col = 'state'
    asset_root_ = 'users/dgketchum/openet/ptjpl/c02'

    for m in ['ptjpl']:
        export_et_fraction(shapefile_path, asset_root_, check_dir=None, grid_spec=3, feature_id=FEATURE_ID,
                           start_yr=2016, end_yr=2024)

# ========================= EOF =======================================================================================
