import os
import sys
import time
from datetime import datetime

import ee
import geopandas as gpd
import openet
from tqdm import tqdm

from openet.ptjpl.ee_utils import is_authorized

sys.path.insert(0, os.path.abspath('../..'))
sys.setrecursionlimit(5000)

LANDSAT_COLLECTIONS = ['LANDSAT/LT04/C02/T1_L2',
                       'LANDSAT/LT05/C02/T1_L2',
                       'LANDSAT/LE07/C02/T1_L2',
                       'LANDSAT/LC08/C02/T1_L2',
                       'LANDSAT/LC09/C02/T1_L2']


def export_et_fraction(shapefile, asset_root, feature_id='FID', select=None, start_yr=2000, end_yr=2024):
    """"""
    df = gpd.read_file(shapefile)
    df = df.set_index(feature_id, drop=False)
    df = df.sample(frac=1)

    original_crs = df.crs
    assert original_crs and original_crs.srs == 'EPSG:4326'

    skipped, exported = 0, 0

    for fid, row in df.iterrows():

        if row['geometry'].geom_type == 'Point':
            polygon = ee.Geometry.Point(
                [row['geometry'].x, row['geometry'].y], proj=original_crs.srs
            ).buffer(100)
        elif row['geometry'].geom_type == 'Polygon':
            polygon = ee.Geometry.Polygon(
                coords=[[c[0], c[1]] for c in row['geometry'].exterior.coords],
                proj=original_crs.srs
            ).buffer(100)
        else:
            skipped += (end_yr - start_yr + 1)
            continue

        for year in range(start_yr, end_yr + 1):

            if select is not None and fid not in select:
                continue

            if row['iso'] not in ['FR', 'BE', 'DE', 'AT', 'IT', 'GR', 'RO', 'ES']:
                continue

            if not (row['glc10_lc'] == 10.0 and row['modis_lc'] == 12.0):
                continue

            if fid == "Delta de l'Ebre":
                continue

            coll = openet.ptjpl.Collection(LANDSAT_COLLECTIONS, start_date=f'{start_yr}-01-01',
                                           end_date=f'{end_yr}-12-31', geometry=polygon,
                                           cloud_cover_max=70)

            scenes = coll.get_image_ids()
            scenes = list(set(scenes))
            scenes = sorted(scenes, key=lambda item: item.split('_')[-1])

            with tqdm(scenes, desc=f'Export PTJPL for {fid}', total=len(scenes)) as pbar:
                for img_id in scenes:
                    pbar.set_description(f'Export PTJPL for {fid} (Processing: {img_id})')
                    splt = img_id.split('/')
                    coll_id = '/'.join(splt[:4])
                    splt = splt[-1].split('_')
                    _name = '_'.join(splt[-3:])
                    tile = splt[-2]
                    p, r = int(tile[:3]), int(tile[-3:])

                    ptjpl_kwargs = dict(ta_source='ERA5LAND',
                                        ea_source='ERA5LAND',
                                        windspeed_source='ERA5LAND',
                                        rs_source='ERA5LAND',
                                        LWin_source='ERA5LAND')

                    etf = openet.ptjpl.Image.from_landsat_c2_sr(img_id,
                                                                et_reference_source='ERA5LAND',
                                                                et_reference_band='eto',
                                                                et_reference_factor=1.0,
                                                                et_reference_resample='bilinear',
                                                                **ptjpl_kwargs
                                                                ).et_fraction

                    info = ee.Image(img_id).getInfo()
                    proj = etf.select('et_fraction').getInfo()['bands'][0]
                    dims = proj['dimensions']
                    crs = proj['crs']
                    crs_transform = proj['crs_transform']
                    dims = f'{dims[0]}x{dims[1]}'

                    properties = {
                        'build_date': datetime.today().strftime('%Y-%m-%d'),
                        'coll_id': coll_id,
                        'core_version': openet.core.__version__,
                        'image_id': img_id,
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

                    etf = etf.set(properties)

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
                    except ee.ee_exception.EEException as e:
                        print('{}, waiting on '.format(e), _name, '......')
                        time.sleep(600)
                        task.start()

                    exported += 1


if __name__ == '__main__':

    is_authorized()

    project = '6_Flux_International'

    root = '/data/ssd2/swim'
    data = os.path.join(root, project, 'data')
    project_ws_ = os.path.join(root, project)
    if not os.path.isdir(root):
        root = '/home/dgketchum/PycharmProjects/swim-rs'
        project_ws_ = os.path.join(root, 'tutorials', project)
        data = os.path.join(project_ws_, 'data')

    shapefile_ = os.path.join(data, 'gis', '6_Flux_International.shp')

    # Volk static footprints
    FEATURE_ID = 'sid'

    # state_col = 'state'
    asset_root_ = 'users/dgketchum/openet/ptjpl/c02'

    sites_ = ["Delta de l'Ebre"]

    for m in ['ptjpl']:
        export_et_fraction(shapefile_, asset_root_, feature_id=FEATURE_ID, start_yr=2015, end_yr=2024,
                           select=None)

# ========================= EOF =======================================================================================
