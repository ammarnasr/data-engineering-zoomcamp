import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    name = name.replace('ID', '_id')
    name = name.replace('PU', 'pu_')
    name = name.replace('DO', 'do_')
    name = name.lower()
    return name


@transformer
def transform(data, *args, **kwargs):
    data = data[data['passenger_count'] != 0]
    data = data[data['trip_distance'] != 0]
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date
    cols_before = data.columns
    data.columns = [camel_to_snake(col) for col in data.columns]
    cols_after = data.columns
    cols_changed = [(c1, c2) for c1, c2 in zip(cols_before, cols_after) if c1 != c2]
    print(f'unique vendors: {data.vendor_id.unique()}')
    print(f'unique days: {len(data.lpep_pickup_date.unique())}')
    print(f'shape after filter: {data.shape}')
    print(f'{len(cols_changed)} columns changed: {cols_changed}')
    return data

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'

@test
def test_vendor_id(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'vendor_id is not in the columns'

@test
def test_passenger_count(output, *args) -> None:
    assert output['passenger_count'].min() > 0, 'passenger_count is not greater than 0'

@test
def test_trip_distance(output, *args) -> None:
    assert output['trip_distance'].min() > 0, 'trip_distance is not greater than 0'