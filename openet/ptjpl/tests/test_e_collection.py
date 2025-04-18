# import pprint

import ee
import pytest

import openet.ptjpl as ptjpl
import openet.ptjpl.utils as utils
# TODO: import utils from openet.core
# import openet.core.utils as utils

C02_COLLECTIONS = ['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2']
# Image LE07_044033_20170724 is not (yet?) in LANDSAT/LE07/C02/T1_L2
# C02_SCENE_ID_LIST = ['LC08_044033_20170716', 'LE07_044033_20170708', 'LE07_044033_20170724']
C02_SCENE_ID_LIST = ['LC08_044033_20170716', 'LE07_044033_20170708']
START_DATE = '2017-07-01'
END_DATE = '2017-08-01'
SCENE_GEOM = (-121.91, 38.99, -121.89, 39.01)
SCENE_POINT = (-121.9, 39)
VARIABLES = {'et', 'et_fraction', 'et_reference'}
TEST_POINT = (-121.5265, 38.7399)

interp_args = {
    'interp_source': 'IDAHO_EPSCOR/GRIDMET',
    'interp_band': 'eto',
    'interp_resample': 'nearest',
}


def default_coll_args():
    return {
        'collections': C02_COLLECTIONS,
        'geometry': ee.Geometry.Point(SCENE_POINT),
        'start_date': START_DATE,
        'end_date': END_DATE,
        'variables': list(VARIABLES),
        'cloud_cover_max': 70,
        'model_args': {
            'et_reference_source': 'IDAHO_EPSCOR/GRIDMET',
            'et_reference_band': 'eto',
            'et_reference_factor': 0.85,
            'et_reference_resample': 'nearest',
            'cloudmask_args': {'cloud_score_flag': False, 'filter_flag': False},
        },
        'filter_args': {},
    }


def default_coll_obj(**kwargs):
    args = default_coll_args().copy()
    args.update(kwargs)
    return ptjpl.Collection(**args)


def parse_scene_id(output_info):
    output = [x['properties']['system:index'] for x in output_info['features']]
    # Strip merge indices (this works for Landsat image IDs
    return sorted(['_'.join(x.split('_')[-3:]) for x in output])


def test_Collection_init_default_parameters():
    """Test if init sets default parameters"""
    args = default_coll_args().copy()
    del args['variables']
    del args['model_args']

    m = ptjpl.Collection(**args)
    assert m.variables is None
    assert m.cloud_cover_max == 70
    assert m.filter_args == {}


def test_Collection_init_collection_str(coll_id='LANDSAT/LC08/C02/T1_L2'):
    """Test if a single coll_id str is converted to a single item list"""
    assert default_coll_obj(collections=coll_id).collections == [coll_id]


def test_Collection_init_cloud_cover_max_str():
    """Test if cloud_cover_max strings are converted to float"""
    assert default_coll_obj(cloud_cover_max='70').cloud_cover_max == 70


@pytest.mark.parametrize(
    'coll_id, start_date, end_date',
    [
        # ['LANDSAT/LT04/C02/T1_L2', '1981-01-01', '1982-01-01'],
        # ['LANDSAT/LT04/C02/T1_L2', '1994-01-01', '1995-01-01'],
        ['LANDSAT/LT05/C02/T1_L2', '1983-01-01', '1984-01-01'],
        ['LANDSAT/LT05/C02/T1_L2', '2012-01-01', '2013-01-01'],
        ['LANDSAT/LE07/C02/T1_L2', '1998-01-01', '1999-01-01'],
        ['LANDSAT/LE07/C02/T1_L2', '2022-01-01', '2023-01-01'],
        ['LANDSAT/LC08/C02/T1_L2', '2012-01-01', '2013-01-01'],
        ['LANDSAT/LC09/C02/T1_L2', '2021-01-01', '2022-01-01'],
    ]
)
def test_Collection_init_collection_filter(coll_id, start_date, end_date):
    """Test that collection IDs are filtered based on start/end dates"""
    # The target collection ID should be removed from the collections lists
    assert default_coll_obj(collections=coll_id, start_date=start_date,
                            end_date=end_date).collections == []


def test_Collection_init_startdate_exception():
    """Test if Exception is raised for invalid start date formats"""
    with pytest.raises(ValueError):
        default_coll_obj(start_date='1/1/2000', end_date='2000-01-02')


def test_Collection_init_enddate_exception():
    """Test if Exception is raised for invalid end date formats"""
    with pytest.raises(ValueError):
        default_coll_obj(start_date='2000-01-01', end_date='1/2/2000')


def test_Collection_init_swapped_date_exception():
    """Test if Exception is raised when start_date == end_date"""
    with pytest.raises(ValueError):
        default_coll_obj(start_date='2017-01-01', end_date='2017-01-01')


def test_Collection_init_invalid_collections_exception():
    """Test if Exception is raised for an invalid collection ID"""
    with pytest.raises(ValueError):
        default_coll_obj(collections=['FOO'])


# # CGM - Test is not needed since there are no real time Collection 2 SR collections
# #   and Collection 1 is no longer supported
# def test_Collection_init_duplicate_collections_exception():
#     """Test if Exception is raised for duplicate Landsat types"""
#     with pytest.raises(ValueError):
#         default_coll_obj(collections=['LANDSAT/LC08/C02/T1_RT_TOA', 'LANDSAT/LC08/C02/T1_TOA'])
#     with pytest.raises(ValueError):
#         default_coll_obj(collections=['LANDSAT/LC08/C02/T1_SR', 'LANDSAT/LC08/C02/T1_TOA'])


def test_Collection_init_cloud_cover_exception():
    """Test if Exception is raised for an invalid cloud_cover_max"""
    with pytest.raises(TypeError):
        default_coll_obj(cloud_cover_max='A')
    with pytest.raises(ValueError):
        default_coll_obj(cloud_cover_max=-1)
    with pytest.raises(ValueError):
        default_coll_obj(cloud_cover_max=101)


def test_Collection_build_default():
    output = utils.getinfo(default_coll_obj()._build())
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output) == C02_SCENE_ID_LIST
    # Check that the variables being set in the default collection object are returned
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


def test_Collection_build_variables_custom(variable='ndvi'):
    # Check that setting the build variables overrides the collection variables
    output = utils.getinfo(default_coll_obj()._build(variables=[variable]).first().bandNames())
    assert set(output) == {variable}


def test_Collection_build_variables_none():
    """Test for exception if variables is set to None in method call"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj(variables=None)._build(variables=None))


def test_Collection_build_variables_not_set():
    """Test for exception if variables is not set in method since default is None"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj(variables=None)._build())


def test_Collection_build_variables_empty_list():
    # Setting variables to an empty list should return the merged Landsat collection
    output = utils.getinfo(
        default_coll_obj(collections=C02_COLLECTIONS, variables=None)
        ._build(variables=[]).first().bandNames()
    )
    assert 'SR_B3' in output


def test_Collection_build_invalid_variable_exception():
    """Test if Exception is raised for an invalid variable"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj()._build(variables=['FOO']))


def test_Collection_build_dates():
    """Check that dates passed to build function override Class dates"""
    coll_obj = default_coll_obj(start_date='2017-08-01', end_date='2017-09-01')
    output = utils.getinfo(coll_obj._build(start_date='2017-07-16', end_date='2017-07-17'))
    assert parse_scene_id(output) == ['LC08_044033_20170716']


def test_Collection_build_landsat_c2_sr():
    """Test if the Landsat SR collections can be built"""
    coll_obj = default_coll_obj(collections=['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2'])
    output = utils.getinfo(coll_obj._build())
    assert parse_scene_id(output) == C02_SCENE_ID_LIST
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


def test_Collection_build_exclusive_enddate():
    """Test if the end_date is exclusive"""
    output = utils.getinfo(default_coll_obj(end_date='2017-07-24')._build())
    assert [x for x in parse_scene_id(output) if int(x[-8:]) >= 20170724] == []


def test_Collection_build_cloud_cover():
    """Test if the cloud cover max parameter is being applied"""
    # CGM - The filtered images should probably be looked up programmatically
    output = utils.getinfo(default_coll_obj(cloud_cover_max=0.5)._build(variables=['et']))
    assert 'LE07_044033_20170724' not in parse_scene_id(output)


@pytest.mark.parametrize(
    'collection_id, start_date, end_date',
    [
        ['LANDSAT/LT05/C02/T1_L2', '2012-01-01', '2013-01-01'],
    ]
)
def test_Collection_build_filter_dates_lt05(collection_id, start_date, end_date):
    """Test that bad Landsat 5 images are filtered"""
    coll_obj = default_coll_obj(
        collections=[collection_id], start_date=start_date, end_date=end_date,
        geometry=ee.Geometry.Rectangle(-125, 25, -65, 50),
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert parse_scene_id(output) == []


@pytest.mark.parametrize(
    'collection_id, start_date, end_date',
    [
        ['LANDSAT/LE07/C02/T1_L2', '2022-01-01', '2023-01-01'],
    ]
)
def test_Collection_build_filter_dates_le07(collection_id, start_date, end_date):
    """Test that Landsat 7 images after 2021 are filtered"""
    coll_obj = default_coll_obj(
        collections=[collection_id], start_date=start_date, end_date=end_date,
        geometry=ee.Geometry.Rectangle(-125, 25, -65, 50),
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert parse_scene_id(output) == []


@pytest.mark.parametrize(
    'collection_id, start_date, end_date',
    [
        ['LANDSAT/LC08/C02/T1_L2', '2013-01-01', '2013-04-01'],
    ]
)
def test_Collection_build_filter_dates_lc08(collection_id, start_date, end_date):
    """Test that pre-op Landsat 8 images before 2013-04-01 are filtered"""
    coll_obj = default_coll_obj(
        collections=[collection_id], start_date=start_date, end_date=end_date,
        geometry=ee.Geometry.Rectangle(-125, 25, -65, 50),
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert not [x for x in parse_scene_id(output) if x.split('_')[-1] < end_date.replace('-', '')]
    assert parse_scene_id(output) == []


@pytest.mark.parametrize(
    'collection_id, start_date, end_date',
    [
        ['LANDSAT/LC09/C02/T1_L2', '2021-11-01', '2022-01-01'],
    ]
)
def test_Collection_build_filter_dates_lc09(collection_id, start_date, end_date):
    """Test that Landsat 9 images before 2022-01-01 are filtered"""
    coll_obj =default_coll_obj(
        collections=[collection_id], start_date=start_date, end_date=end_date,
        geometry=ee.Geometry.Rectangle(-125, 25, -65, 50),
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert not [x for x in parse_scene_id(output) if x.split('_')[-1] < end_date.replace('-', '')]
    assert parse_scene_id(output) == []


def test_Collection_build_filter_args_keyword():
    # Need to test with two collections to catch bug when deepcopy isn't used
    collection_ids = ['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2']
    wrs2_filter = [
        {'type': 'equals', 'leftField': 'WRS_PATH', 'rightValue': 44},
        {'type': 'equals', 'leftField': 'WRS_ROW', 'rightValue': 33},
    ]
    coll_obj = default_coll_obj(
        collections=collection_ids,
        geometry=ee.Geometry.Rectangle(-125, 35, -120, 40),
        filter_args={c: wrs2_filter for c in collection_ids},
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert {x[5:11] for x in parse_scene_id(output)} == {'044033'}


def test_Collection_build_filter_args_eeobject():
    # Need to test with two collections to catch bug when deepcopy isn't used
    collection_ids = ['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2']
    wrs2_filter = ee.Filter.And(ee.Filter.equals('WRS_PATH', 44), ee.Filter.equals('WRS_ROW', 33))
    coll_obj = default_coll_obj(
        collections=collection_ids,
        geometry=ee.Geometry.Rectangle(-125, 35, -120, 40),
        filter_args={c: wrs2_filter for c in collection_ids},
    )
    output = utils.getinfo(coll_obj._build(variables=['et']))
    assert {x[5:11] for x in parse_scene_id(output)} == {'044033'}


def test_Collection_overpass_default():
    """Test overpass method with default values (variables from Class init)"""
    output = utils.getinfo(default_coll_obj().overpass())
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES
    assert parse_scene_id(output) == C02_SCENE_ID_LIST


def test_Collection_overpass_class_variables():
    """Test that custom class variables are passed through to build function"""
    output = utils.getinfo(default_coll_obj(variables=['et']).overpass())
    assert {y['id'] for x in output['features'] for y in x['bands']} == {'et'}


def test_Collection_overpass_method_variables():
    """Test that custom method variables are passed through to build function"""
    output = utils.getinfo(default_coll_obj().overpass(variables=['et']))
    assert {y['id'] for x in output['features'] for y in x['bands']} == {'et'}


def test_Collection_overpass_no_variables_exception():
    """Test if Exception is raised if variables is not set in init or method"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj(variables=[]).overpass())


def test_Collection_interpolate_default():
    """Default t_interval should be custom"""
    output = utils.getinfo(default_coll_obj().interpolate(**interp_args))
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output) == [START_DATE.replace('-', '')]
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


@pytest.mark.parametrize('use_joins', [True, False])
def test_Collection_interpolate_use_joins(use_joins):
    """Only checking if the parameter is accepted and runs for now"""
    output = utils.getinfo(default_coll_obj().interpolate(use_joins=use_joins, **interp_args))
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output) == ['20170701']


def test_Collection_interpolate_variables_custom_et():
    output = utils.getinfo(default_coll_obj().interpolate(variables=['et'], **interp_args))
    assert {y['id'] for x in output['features'] for y in x['bands']} == {'et'}


def test_Collection_interpolate_t_interval_daily():
    """Test if the daily time interval parameter works

    Since end_date is exclusive last image date will be one day earlier
    """
    coll_obj = default_coll_obj(start_date='2017-07-01', end_date='2017-07-05')
    output = utils.getinfo(coll_obj.interpolate(t_interval='daily', **interp_args))
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output)[0] == '20170701'
    assert parse_scene_id(output)[-1] == '20170704'
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


def test_Collection_interpolate_t_interval_monthly():
    """Test if the monthly time interval parameter works"""
    output = utils.getinfo(default_coll_obj().interpolate(t_interval='monthly', **interp_args))
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output) == ['201707']
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


def test_Collection_interpolate_t_interval_custom():
    """Test if the custom time interval parameter works"""
    output = utils.getinfo(default_coll_obj().interpolate(t_interval='custom', **interp_args))
    assert output['type'] == 'ImageCollection'
    assert parse_scene_id(output) == ['20170701']
    assert {y['id'] for x in output['features'] for y in x['bands']} == VARIABLES


def test_Collection_interpolate_t_interval_exception():
    """Test if Exception is raised for an invalid t_interval parameter"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj().interpolate(t_interval='DEADBEEF'))


def test_Collection_interpolate_interp_method_exception():
    """Test if Exception is raised for an invalid interp_method parameter"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj().interpolate(interp_method='DEADBEEF'))


def test_Collection_interpolate_interp_days_exception():
    """Test if Exception is raised for an invalid interp_days parameter"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj().interpolate(interp_days=0))


def test_Collection_interpolate_no_variables_exception():
    """Test if Exception is raised if variables is not set in init or method"""
    with pytest.raises(ValueError):
        utils.getinfo(default_coll_obj(variables=[]).interpolate())


def test_Collection_interpolate_output_type_default():
    """Test if output_type parameter is defaulting to float"""
    vars = ['et', 'et_reference', 'et_fraction', 'count']
    output = utils.getinfo(default_coll_obj(variables=vars).interpolate(**interp_args))
    output = output['features'][0]['bands']
    bands = {info['id']: i for i, info in enumerate(output)}
    assert output[bands['et']]['data_type']['precision'] == 'float'
    assert output[bands['et_reference']]['data_type']['precision'] == 'float'
    assert output[bands['et_fraction']]['data_type']['precision'] == 'float'
    assert output[bands['count']]['data_type']['precision'] == 'int'


def test_Collection_interpolate_only_interpolate_images():
    """Test if count band is returned if no images in the date range"""
    variables = {'et', 'count'}
    output = utils.getinfo(default_coll_obj(
        collections=['LANDSAT/LC08/C02/T1_L2'],
        geometry=ee.Geometry.Point(-123.623, 44.745),
        start_date='2017-04-01', end_date='2017-04-30',
        variables=list(variables), cloud_cover_max=70).interpolate(**interp_args))
    assert {y['id'] for x in output['features'] for y in x['bands']} == variables


# # TODO: Write a test to see if et_fraction_max is being applied
# def test_Collection_interpolate_et_fraction_max():
#     custom_args = interp_args.copy()
#     custom_args['et_fraction_max'] = 1.4
#     output = utils.getinfo(default_coll_obj().interpolate(**custom_args))
#     assert ?


@pytest.mark.parametrize(
    'collections, scene_id_list',
    [
        [['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2'], C02_SCENE_ID_LIST],
    ]
)
def test_Collection_get_image_ids(collections, scene_id_list):
    # get_image_ids method makes a getInfo call internally
    output = default_coll_obj(collections=collections, variables=None).get_image_ids()
    assert type(output) is list
    assert {x.split('/')[-1] for x in output} == set(scene_id_list)
