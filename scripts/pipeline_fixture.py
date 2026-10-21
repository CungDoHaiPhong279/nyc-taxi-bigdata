"""Offline P6.3 fixture/oracle; sent on stdin to the immutable clean worker.

This is a test harness, not another ETL implementation. Only the existing CLIs
transform/load data. Expected values below are specified independently of ETL.
Never use this harness against the current/production environment.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import socket
import sys
import traceback
import unittest
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path('/app')
DATA = ROOT / 'data'
REPORTS = ROOT / 'reports'
STATE = REPORTS / 'p63_fixture_state.json'
FIXTURE_ID = 'docker-p63-synthetic-v1'
VERIFIED_IMAGE = 'sha256:32f066cdb5e0827d3b45abc387972ca45650cf43ed05e602cdcb6287dff43cd4'
MONTHS = tuple(f'2025-{m:02d}' for m in range(1, 7))
COUNTS = {'2025-01': 4, '2025-02': 3, '2025-03': 3, '2025-04': 3, '2025-05': 3, '2025-06': 4}
RAW_COUNTS = {'2025-01': 14, '2025-02': 3, '2025-03': 3, '2025-04': 3, '2025-05': 3, '2025-06': 4}
EXPECTED = {'raw_taxi': 30, 'accepted': 20, 'invalid': 9, 'duplicates': 1,
            'fare_sum': 80.0, 'fare_average': 4.0, 'distance_sum': 30.0,
            'distance_average': 1.5, 'negative_fare': 6, 'zero_fare': 6,
            'null_passengers': 6, 'calendar_null_fk': 1, 'weather_null_fk': 2,
            'holiday_trips': 6, 'rainy_trips': 1, 'null_temperature_trips': 3}
HOLIDAYS = {
    '2025-01-01': "New Year's Day", '2025-01-20': 'Birthday of Martin Luther King, Jr.',
    '2025-02-17': "Washington's Birthday", '2025-05-26': 'Memorial Day',
    '2025-06-19': 'Juneteenth National Independence Day', '2025-07-04': 'Independence Day',
    '2025-09-01': 'Labor Day', '2025-10-13': 'Columbus Day', '2025-11-11': 'Veterans Day',
    '2025-11-27': 'Thanksgiving Day', '2025-12-25': 'Christmas Day',
}
# month, NYC wall clock, base fare USD, distance miles, passengers, expected UTC hour.
# UTC hour values are literal checkpoints, not obtained from Spark or Gold joins.
CASES = (
    (1, '2025-01-20T10:00', 10., 1.5, 1, '2025-01-20T15:00'),
    (1, '2025-01-20T11:00', -5., 0., None, '2025-01-20T16:00'),
    (1, '2025-01-20T12:00', 0., 2., 0, '2025-01-20T17:00'),
    (1, '2007-01-01T12:00', 20., 4., 1, None),
    (2, '2025-02-15T10:00', 10., 1.5, 1, '2025-02-15T15:00'),
    (2, '2025-02-15T11:00', -5., 0., None, '2025-02-15T16:00'),
    (2, '2025-02-15T12:00', 0., 2., 0, '2025-02-15T17:00'),
    (3, '2025-03-15T10:00', 10., 1.5, 1, '2025-03-15T14:00'),
    (3, '2025-03-15T11:00', -5., 0., None, '2025-03-15T15:00'),
    (3, '2025-03-15T12:00', 0., 2., 0, '2025-03-15T16:00'),
    (4, '2025-04-15T10:00', 10., 1.5, 1, '2025-04-15T14:00'),
    (4, '2025-04-15T11:00', -5., 0., None, '2025-04-15T15:00'),
    (4, '2025-04-15T12:00', 0., 2., 0, '2025-04-15T16:00'),
    (5, '2025-05-15T10:00', 10., 1.5, 1, '2025-05-15T14:00'),
    (5, '2025-05-15T11:00', -5., 0., None, '2025-05-15T15:00'),
    (5, '2025-05-15T12:00', 0., 2., 0, '2025-05-15T16:00'),
    (6, '2025-06-19T10:00', 10., 1.5, 1, '2025-06-19T14:00'),
    (6, '2025-06-19T11:00', -5., 0., None, '2025-06-19T15:00'),
    (6, '2025-06-19T12:00', 0., 2., 0, '2025-06-19T16:00'),
    (6, '2025-06-30T23:00', 30., 5., 1, None),
)
RULES = ('pickup_timestamp_null', 'dropoff_timestamp_null', 'invalid_duration',
         'pickup_location_null', 'dropoff_location_null', 'trip_distance_null',
         'trip_distance_negative', 'fare_amount_null', 'passenger_count_negative')
RENAMES = {'VendorID': 'vendor_id', 'tpep_pickup_datetime': 'pickup_datetime_local',
           'tpep_dropoff_datetime': 'dropoff_datetime_local', 'RatecodeID': 'ratecode_id',
           'PULocationID': 'pickup_location_id', 'DOLocationID': 'dropoff_location_id',
           'Airport_fee': 'airport_fee'}
TABLE_LIMITS = {'dim_location': 265, 'dim_calendar': 365, 'dim_weather': 4344,
                'fact_trip': 20, 'etl_load_log': 6}
INDEXES = {'fact_trip_pickup_datetime_idx', 'fact_trip_pickup_location_idx',
           'fact_trip_dropoff_location_idx', 'fact_trip_calendar_date_idx',
           'fact_trip_weather_idx', 'fact_trip_pickup_hour_idx',
           'dim_weather_local_hour_idx', 'fact_trip_source_month_idx'}


class FixtureError(RuntimeError):
    """Safe fixed-message assertion failure; never contains connection values."""


def require(condition, label):
    if not condition:
        raise FixtureError(label)


def normalized(value):
    if isinstance(value, dict):
        return {k: normalized(v.replace(tzinfo=timezone.utc) if
                k.endswith('timestamp_utc') and isinstance(v, datetime) and v.tzinfo is None
                else v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def canonical(value):
    return json.dumps(normalized(value), sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


SPEC_HASH = digest({'cases': CASES, 'expected': EXPECTED, 'holidays': HOLIDAYS, 'version': FIXTURE_ID})


def equal(actual, expected, label):
    """Independent floating comparisons: abs/relative tolerance 1e-9."""
    a, e = normalized(actual), normalized(expected)
    if isinstance(e, dict):
        require(isinstance(a, dict) and set(a) == set(e), label + ' fields')
        for k in e:
            equal(a[k], e[k], label + '/' + k)
    elif isinstance(e, list):
        require(isinstance(a, list) and len(a) == len(e), label + ' length')
        for index, (left, right) in enumerate(zip(a, e)):
            equal(left, right, f'{label}/{index}')
    elif isinstance(e, float):
        require(isinstance(a, (int, float)) and math.isclose(a, e, rel_tol=1e-9, abs_tol=1e-9), label)
    else:
        require(a == e, label)


def write_owned_json(path, payload):
    require(path.parent == REPORTS, 'Report path outside fixture reports')
    path.write_text(json.dumps(normalized(payload), indent=2, sort_keys=True) + '\n', encoding='utf-8')


def marker():
    if not STATE.exists():
        return None
    state = json.loads(STATE.read_text(encoding='utf-8'))
    require(state.get('fixture_id') == FIXTURE_ID and state.get('spec_hash') == SPEC_HASH,
            'Runtime is not owned by this exact fixture specification')
    require(state.get('image_id') == VERIFIED_IMAGE, 'Fixture image differs from verified clean image')
    expected_paths = {f'raw/taxi/yellow_tripdata_{month}.parquet' for month in MONTHS}
    expected_paths.update({'raw/raw_manifest.json','raw/calendar/calendar_2025.csv',
                           'raw/taxi_zones/taxi_zone_lookup.csv',
                           'raw/weather/weather_nyc_2025-01_2025-06.json'})
    require(set(state.get('raw_hashes',{})) == expected_paths, 'Fixture raw inventory is not the exact public allowlist')
    actual_paths = {p.relative_to(DATA).as_posix() for p in (DATA/'raw').rglob('*') if p.is_file()}
    require(actual_paths == expected_paths, 'Unknown files in owned fixture raw runtime')
    require({p.name for p in DATA.iterdir()}.issubset({'raw','bronze','silver','gold'}),
            'Unknown data runtime entries; nothing will be deleted')
    require({p.name for p in REPORTS.iterdir()}.issubset({STATE.name,'p63_expected.json',
            'p63_files_baseline.json','p63_database_baseline.json','p63_fixture_report.json',
            'postgres_load_report.json'}), 'Unknown report runtime entries; refusing overwrite')
    for relative, expected_sha in state['raw_hashes'].items():
        path = DATA / relative
        require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha,
                'Fixture raw inputs changed; refusing overwrite/reload')
    return state


def raw_case(case):
    _, stamp, fare, distance, passengers, _ = case
    pickup = datetime.fromisoformat(stamp)
    return {'VendorID': 1, 'tpep_pickup_datetime': pickup,
            'tpep_dropoff_datetime': pickup + timedelta(minutes=10),
            'passenger_count': passengers, 'trip_distance': distance, 'RatecodeID': 1,
            'store_and_fwd_flag': 'N', 'PULocationID': 1, 'DOLocationID': 2,
            'payment_type': None if passengers is None else 2, 'fare_amount': fare,
            'extra': .5, 'mta_tax': .5, 'tip_amount': 1., 'tolls_amount': 0.,
            'improvement_surcharge': 0., 'total_amount': fare + 2.,
            'congestion_surcharge': 0., 'Airport_fee': 0., 'cbd_congestion_fee': 0.}


def raw_rows(month):
    rows = [raw_case(c) for c in CASES if c[0] == month]
    if month == 1:
        base = raw_case(CASES[0])
        alterations = [('tpep_pickup_datetime', None), ('tpep_dropoff_datetime', None),
                       ('tpep_dropoff_datetime', base['tpep_pickup_datetime']),
                       ('PULocationID', None), ('DOLocationID', None),
                       ('trip_distance', None), ('trip_distance', -1.),
                       ('fare_amount', None), ('passenger_count', -1)]
        rows.extend({**base, field: value} for field, value in alterations)
        rows.append(dict(base))  # Exact business duplicate, no artificial case-id column.
    return rows


def expected_zone(number):
    return {'location_id': number,
            'borough': 'Fixture Manhattan' if number == 1 else 'Fixture Queens' if number == 2 else 'Fixture Other',
            'zone': f'Synthetic Zone {number:03d}',
            'service_zone': None if number == 265 else 'Fixture Service'}


def expected_calendar(day):
    return {'date_id': day, 'day_of_week': day.weekday(), 'day_name': day.strftime('%A'),
            'month': day.month, 'year': day.year, 'is_weekend': day.weekday() >= 5,
            'is_holiday': day.isoformat() in HOLIDAYS, 'holiday_name': HOLIDAYS.get(day.isoformat())}


def expected_weather(utc):
    # Python zoneinfo is an independent timezone oracle (Spark uses JVM rules).
    local = utc.astimezone(ZoneInfo('America/New_York')).replace(tzinfo=None)
    rainy = utc == datetime(2025, 1, 20, 15, tzinfo=timezone.utc)
    return {'weather_timestamp_utc': utc, 'weather_timestamp_nyc': local,
            'weather_date_nyc': local.date(), 'weather_hour_nyc': local.hour,
            'temperature_2m': None if utc == datetime(2025, 2, 15, 16, tzinfo=timezone.utc) else 12.5,
            'precipitation': 2.5 if rainy else 0., 'snowfall': 0., 'wind_speed_10m': 8.,
            'weather_code': 61 if rainy else 0}


def all_weather():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [expected_weather(start + timedelta(hours=i)) for i in range(4344)]


def expected_silver(case):
    row = {RENAMES.get(k, k): v for k, v in raw_case(case).items()}
    pickup = row['pickup_datetime_local']
    return {**row, 'pickup_date': pickup.date(), 'pickup_hour': pickup.hour}


def expected_gold(case):
    row = expected_silver(case)
    pickup = row['pickup_datetime_local']
    holiday = HOLIDAYS.get(pickup.date().isoformat())
    calendar_match = pickup.year == 2025
    utc = datetime.fromisoformat(case[5]).replace(tzinfo=timezone.utc) if case[5] else None
    weather = expected_weather(utc) if utc else {key: None for key in expected_weather(datetime(2025, 1, 1, tzinfo=timezone.utc))}
    # Gold projects weather values, not the temporary date/hour join keys.
    weather = {key:value for key,value in weather.items() if key not in ('weather_date_nyc','weather_hour_nyc')}
    return {**row, **weather,
            **{f'{side}_{key}': value for side, zone in [('pickup', expected_zone(1)), ('dropoff', expected_zone(2))]
               for key, value in zone.items() if key != 'location_id'},
            'day_of_week': pickup.weekday() if calendar_match else None,
            'is_weekend': pickup.weekday() >= 5 if calendar_match else None,
            'is_holiday': bool(holiday) if calendar_match else None, 'holiday_name': holiday,
            'pickup_zone_matched': True, 'dropoff_zone_matched': True,
            'calendar_matched': calendar_match, 'weather_matched': utc is not None,
            'weather_unmatched_reason': None if utc else 'out_of_analysis_period' if pickup.year != 2025 else 'weather_edge_coverage'}


def permit_counts(counts, owned):
    require(set(counts) == set(TABLE_LIMITS), 'Unexpected database table inventory')
    require(all(0 <= counts[k] <= TABLE_LIMITS[k] for k in counts), 'Database exceeds fixture cardinality')
    require(not any(counts.values()) or owned, 'Database contains data without fixture ownership')
    dims = [counts[k] for k in ('dim_location', 'dim_calendar', 'dim_weather')]
    require(dims in ([0, 0, 0], [265, 365, 4344]), 'Partial/foreign dimensions; refusing loader')


def database_snapshot(require_complete=False):
    import psycopg
    from psycopg.rows import dict_row
    from nyc_taxi.common.config import load_config
    config = load_config()['database']
    require(config['host'] == 'postgres' and int(config['port']) == 5432
            and config['name'] == 'nyc_taxi_bootstrap_test' and config['user'] == 'bootstrap_test',
            'Only the isolated bootstrap-test database is permitted')
    expected_ip = os.environ.get('P63_EXPECTED_POSTGRES_IP')
    require(bool(expected_ip), 'Missing isolated PostgreSQL identity checkpoint')
    state = marker()
    with psycopg.connect(host=config['host'], port=5432, dbname=config['name'],
                         user=config['user'], password=config['password'],
                         connect_timeout=5, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION READ ONLY')
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute('SELECT current_database() AS db, host(inet_server_addr()) AS ip, inet_server_port() AS port')
            identity = cursor.fetchone()
            require(identity == {'db': 'nyc_taxi_bootstrap_test', 'ip': expected_ip, 'port': 5432}
                    and socket.gethostbyname('postgres') == expected_ip, 'PostgreSQL endpoint identity mismatch')
            cursor.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2")
            require({(r['schemaname'], r['tablename']) for r in cursor.fetchall()} ==
                    {('analytics', name) for name in TABLE_LIMITS}, 'Unexpected user tables; fixture guard stopped')
            counts = {}
            for name in TABLE_LIMITS:  # Trusted fixed allowlist, queries intentionally sequential.
                cursor.execute(f'SELECT COUNT(*) AS n FROM analytics.{name}')
                counts[name] = cursor.fetchone()['n']
            permit_counts(counts, state is not None)
            require(not require_complete or counts == TABLE_LIMITS, 'Incomplete fixture database')
            result = {'counts': counts, 'dimensions': {}, 'facts': [], 'logs': []}
            if not any(counts.values()):
                return normalized(result)
            for name, order in [('dim_location', 'location_id'), ('dim_calendar', 'date_id'), ('dim_weather', 'weather_timestamp_utc')]:
                # Explicit column projections; at most 4,344 synthetic dimension rows.
                columns = list(expected_zone(1) if name == 'dim_location' else expected_calendar(date(2025,1,1))
                               if name == 'dim_calendar' else {'weather_id': None, **expected_weather(datetime(2025,1,1,tzinfo=timezone.utc))})
                cursor.execute(f"SELECT {', '.join(columns)} FROM analytics.{name} ORDER BY {order}")
                result['dimensions'][name] = cursor.fetchall()
            assert_dimensions(result['dimensions'])
            projected = expected_fact(CASES[0], {}, check_lineage=False)
            columns = ['trip_id', 'weather_id', *projected]
            cursor.execute('SELECT ' + ', '.join('f.' + key for key in columns) +
                           ', w.weather_timestamp_utc FROM analytics.fact_trip f LEFT JOIN analytics.dim_weather w USING(weather_id) ORDER BY source_month,pickup_datetime_local')
            result['facts'] = cursor.fetchall()
            cursor.execute('SELECT dataset,source_year,source_month,gold_batch_id,source_row_count,loaded_row_count,started_at,completed_at,status FROM analytics.etl_load_log ORDER BY source_year,source_month')
            result['logs'] = cursor.fetchall()
            assert_fact_ownership(result, require_complete=require_complete)
            cursor.execute("SELECT indexname FROM pg_indexes WHERE schemaname='analytics' ORDER BY indexname")
            result['indexes'] = [r['indexname'] for r in cursor.fetchall()]
            cursor.execute("SELECT to_regclass('analytics.vw_trip_enriched') IS NOT NULL AS present")
            present = cursor.fetchone()['present']
            result['view_rows'] = None
            if present:
                cursor.execute('SELECT COUNT(*) AS n FROM analytics.vw_trip_enriched')
                result['view_rows'] = cursor.fetchone()['n']
            require(not require_complete or present and result['view_rows']==20
                    and INDEXES.issubset(result['indexes']), 'Actual indexes/view row preservation failed')
            return normalized(result)


def expected_fact(case, gold, check_lineage=True):
    enriched = expected_gold(case)
    row = {key: enriched[key] for key in expected_silver(case)}
    row.update(source_year=2025, source_month=case[0], pickup_zone_matched=True,
               dropoff_zone_matched=True, calendar_matched=enriched['calendar_matched'],
               weather_matched=enriched['weather_matched'], weather_unmatched_reason=enriched['weather_unmatched_reason'],
               calendar_date_id=enriched['pickup_date'] if enriched['calendar_matched'] else None)
    if check_lineage:
        entry = gold[f'taxi_2025-{case[0]:02d}']
        row.update(_source_file=f'data/raw/taxi/yellow_tripdata_2025-{case[0]:02d}.parquet',
                   _ingestion_batch_id=entry['source_lineage']['bronze_batch_id'],
                   _silver_batch_id=entry['source_silver_batch_id'], _gold_batch_id=entry['gold_batch_id'])
    else:
        row.update(_source_file='', _ingestion_batch_id='', _silver_batch_id='', _gold_batch_id='')
    return row


def assert_dimensions(dimensions):
    equal(dimensions['dim_location'], [expected_zone(i) for i in range(1, 266)], 'Location UPSERT content')
    equal(dimensions['dim_calendar'], [expected_calendar(date(2025,1,1)+timedelta(days=i)) for i in range(365)], 'Calendar content/11 holidays')
    weather = [{k:v for k,v in row.items() if k != 'weather_id'} for row in dimensions['dim_weather']]
    equal(weather, all_weather(), 'Weather UTC/NYC/DST/nullable content')


def manifests(layer):
    from nyc_taxi.ingestion.manifest import load_manifest
    entries = load_manifest(DATA / layer / f'{layer}_manifest.json')
    require(len({e['dataset_name'] for e in entries}) == len(entries), 'Duplicate manifest entries')
    return {e['dataset_name']: e for e in entries}


def assert_fact_ownership(snapshot, require_complete):
    gold = manifests('gold')
    require(set(gold) == {f'taxi_{m}' for m in MONTHS}, 'Complete fixture Gold lineage missing')
    actual_by_month = {}
    for row in snapshot['facts']:
        require(row['source_year'] == 2025 and 1 <= row['source_month'] <= 6, 'Foreign fact partition')
        actual_by_month.setdefault(row['source_month'], []).append(row)
    log_by_month = {}
    for log in snapshot['logs']:
        require(log['dataset'] == 'fact_trip' and log['source_year'] == 2025
                and 1 <= log['source_month'] <= 6, 'Foreign load log')
        log_by_month[log['source_month']] = log
    require(set(actual_by_month) == set(log_by_month), 'Log/physical partition mismatch; refusing reload')
    require(not require_complete or set(actual_by_month) == set(range(1,7)), 'Missing fixture months')
    for month, actual in actual_by_month.items():
        cases = sorted((c for c in CASES if c[0] == month), key=lambda c: c[1])
        require(len(actual) == COUNTS[f'2025-{month:02d}'], 'Incomplete/foreign fact partition')
        for row, case in zip(actual, cases):
            expected = expected_fact(case, gold)
            equal({k:row[k] for k in expected}, expected, 'Fact business values/lineage')
            equal(row['weather_timestamp_utc'], expected_gold(case)['weather_timestamp_utc'], 'Weather FK join')
            require((row['weather_id'] is not None) == expected_gold(case)['weather_matched'], 'Weather FK NULL semantics')
        log = log_by_month[month]
        require(log['status'] == 'loaded' and log['gold_batch_id'] == gold[f'taxi_2025-{month:02d}']['gold_batch_id']
                and log['source_row_count'] == len(actual) == log['loaded_row_count']
                and log['started_at'] is not None and log['completed_at'] is not None,
                'Load log differs from actual fixture/Gold lineage; refusing reload')


def guard():
    state = marker()
    if state is None:
        require(not any(DATA.iterdir()), 'Nonempty data runtime has no fixture ownership')
        require(not any(REPORTS.iterdir()), 'Nonempty reports runtime has no fixture ownership')
    require(not any((ROOT/'models').iterdir()), 'Unexpected fixture model files')
    snapshot = database_snapshot()
    baseline = REPORTS / 'p63_database_baseline.json'
    if baseline.exists():
        equal(snapshot, json.loads(baseline.read_text()), 'Existing fixture database changed; refusing reload')
    print(json.dumps({'guard': 'pass', 'database_counts': snapshot['counts'], 'owned_fixture': state is not None}))


def prepare():
    guard()
    if marker() is not None:
        print(json.dumps({'prepare': 'existing fixture inputs retained', 'expected': EXPECTED}))
        return
    import pyarrow as pa
    import pyarrow.parquet as pq
    from nyc_taxi.ingestion.manifest import build_manifest_entry, save_manifest
    from nyc_taxi.ingestion.validation import validate_calendar, validate_taxi_parquet, validate_taxi_zones, validate_weather
    schema = pa.schema([(key, pa.timestamp('us') if key.startswith('tpep_') else pa.string()
                         if key == 'store_and_fwd_flag' else pa.int64()
                         if key in ('VendorID','passenger_count','RatecodeID','PULocationID','DOLocationID','payment_type')
                         else pa.float64()) for key in raw_case(CASES[0])])
    sources = []
    for month in range(1, 7):
        path = DATA / 'raw' / 'taxi' / f'yellow_tripdata_2025-{month:02d}.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        require(not path.exists(), 'Refusing existing raw Parquet overwrite')
        pq.write_table(pa.Table.from_pylist(raw_rows(month), schema=schema), path)
        sources.append((f'yellow_taxi_2025-{month:02d}', path, 'parquet', validate_taxi_parquet(path)))
    path = DATA/'raw'/'taxi_zones'/'taxi_zone_lookup.csv'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream); writer.writerow(['LocationID','Borough','Zone','service_zone'])
        for number in range(1,266):
            row = expected_zone(number)
            writer.writerow([number, ' '+row['borough']+' ', ' '+row['zone']+' ',
                             ' '+row['service_zone']+' ' if row['service_zone'] else None])
    sources.append(('taxi_zone_lookup', path, 'csv', validate_taxi_zones(path)))
    path = DATA/'raw'/'calendar'/'calendar_2025.csv'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerow(['date','day_of_week','day_name','month','year','is_weekend','is_holiday','holiday_name'])
        for index in range(365):
            day = date(2025,1,1)+timedelta(days=index)
            writer.writerow([day.isoformat(),day.weekday(),day.strftime('%A'),day.month,2025,
                             str(day.weekday()>=5).lower(),'false',''])
    sources.append(('calendar_2025',path,'csv',validate_calendar(path)))
    path = DATA/'raw'/'weather'/'weather_nyc_2025-01_2025-06.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    weather = all_weather()
    payload = {'timezone':'GMT','utc_offset_seconds':0,'hourly':{
        'time':[r['weather_timestamp_utc'].strftime('%Y-%m-%dT%H:%M') for r in weather],
        **{k:[r[k] for r in weather] for k in ('temperature_2m','precipitation','snowfall','wind_speed_10m','weather_code')}}}
    with path.open('x', encoding='utf-8') as stream:
        json.dump(payload,stream)
    sources.append(('weather_nyc_2025-01_2025-06',path,'json',validate_weather(path,expected_start=date(2025,1,1),expected_end=date(2025,6,30))))
    entries = [build_manifest_entry(name,'SYNTHETIC P6.3 - NOT ACQUIRED',None,path,format,validation)
               for name,path,format,validation in sources]
    save_manifest(DATA/'raw'/'raw_manifest.json', entries)
    raw_hashes = {p.relative_to(DATA).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in (DATA/'raw').rglob('*') if p.is_file()}
    write_owned_json(STATE, {'fixture_id':FIXTURE_ID,'spec_hash':SPEC_HASH,'image_id':VERIFIED_IMAGE,'raw_hashes':raw_hashes})
    write_owned_json(REPORTS/'p63_expected.json', {'fixture_id':FIXTURE_ID,'expected':EXPECTED,
                                                'monthly_raw':RAW_COUNTS,'monthly_accepted':COUNTS,
                                                'dimension_counts':{'location':265,'calendar':365,'weather':4344}})
    print(json.dumps({'prepare':'created synthetic sources only','expected':EXPECTED}))


def parquet_rows(path, columns=None):
    import pyarrow.dataset as ds
    return ds.dataset(str(path),format='parquet').to_table(columns=columns).to_pylist()


def files_snapshot():
    require(marker() is not None, 'Fixture marker missing')
    inventories = {layer:manifests(layer) for layer in ('bronze','silver','gold')}
    names = {f'taxi_{m}' for m in MONTHS}
    for layer, entries in inventories.items():
        require(set(entries) == (names if layer == 'gold' else names|{'calendar','weather','taxi-zones'}), 'Missing/extra fixture datasets')
        for name, entry in entries.items():
            require(entry['validation_status'] == 'valid', 'Invalid fixture manifest')
            count = (RAW_COUNTS if layer=='bronze' else COUNTS)[name[5:]] if name.startswith('taxi_2025') else {'calendar':365,'weather':4344,'taxi-zones':265}[name]
            require(entry[layer+'_rows'] == count, 'Manifest row count differs from independent expectation')
            rows = parquet_rows(ROOT/entry[layer+'_path'])
            require(len(rows) == count, 'Physical Parquet count differs from independent expectation')
            if name.startswith('taxi_2025'):
                month = int(name[-2:])
                if layer == 'bronze':
                    actual = [{k:r[k] for k in raw_case(CASES[0])} for r in rows]
                    equal(sorted(actual,key=canonical),sorted(raw_rows(month),key=canonical),'Bronze preserves raw dirty/NULL/duplicate values')
                else:
                    cases = sorted((c for c in CASES if c[0]==month), key=lambda c:c[1])
                    rows.sort(key=lambda r:r['pickup_datetime_local'])
                    for row, case in zip(rows,cases):
                        expected = expected_silver(case) if layer=='silver' else expected_gold(case)
                        equal({k:row[k] for k in expected}, expected, f'{layer} explicit transform/join values')
                        require(row['_source_year']==2025 and row['_source_month']==month,'Physical source partition metadata')
                        field = '_silver_batch_id' if layer=='silver' else '_gold_batch_id'
                        require(row[field]==entry[field[1:]],'Physical output batch differs from manifest')
                if layer == 'silver':
                    require(entry['invalid_rows']==(9 if month==1 else 0)
                            and entry['duplicate_rows']==(1 if month==1 else 0)
                            and entry['rejected_rows']==(10 if month==1 else 0), 'Silver rejection accounting')
                    equal(entry['rule_counts'],{rule:1 if month==1 else 0 for rule in RULES},'All nine real reject rules')
                if layer == 'gold':
                    for key, unmatched in [('pickup_zone',0),('dropoff_zone',0),('calendar',1 if month==1 else 0),('weather',1 if month in (1,6) else 0)]:
                        equal(entry['join_counts'][key],{'matched':count-unmatched,'unmatched':unmatched},'Gold match flags')
                        equal(entry['join_stage_counts'][key],{'before':count,'after':count},'Gold LEFT JOIN no fanout/loss')
                    equal(entry['weather_unmatched_reasons'],{'out_of_analysis_period':1 if month==1 else 0,
                          'weather_edge_coverage':1 if month==6 else 0,'unexpected_missing_weather_key':0},'Weather missing reasons')
                    require(entry['holiday_trip_count']==(3 if month in (1,6) else 0)
                            and entry['rainy_trip_count']==(1 if month==1 else 0)
                            and entry['weather_null_statistics']['temperature_2m']==(1 if month in (1,2,6) else 0),
                            'Gold holiday/rain/nullable weather aggregates')
    silver = inventories['silver']
    for name, expected in [('taxi-zones',[expected_zone(i) for i in range(1,266)]),
                           ('calendar',[{**expected_calendar(date(2025,1,1)+timedelta(days=i))} for i in range(365)]),
                           ('weather',all_weather())]:
        rows = parquet_rows(ROOT/silver[name]['silver_path'])
        if name == 'calendar':
            for row in rows:
                row['date_id'] = row['date']
        key = 'location_id' if name=='taxi-zones' else 'date_id' if name=='calendar' else 'weather_timestamp_utc'
        rows.sort(key=lambda r:r[key])
        equal([{k:r[k] for k in expected[0]} for r in rows],expected,'Silver full dimension content')
    files = {p.relative_to(DATA).as_posix():{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
             'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns}
             for p in DATA.rglob('*') if p.is_file() and (p.suffix=='.parquet' or p.name=='_SUCCESS' or p.parent.name in ('calendar','weather','taxi_zones') and 'raw' in p.parts)}
    return {'manifests':inventories,'physical_files':files}


def update_report(phase, payload):
    path = REPORTS/'p63_fixture_report.json'
    report = json.loads(path.read_text()) if path.exists() else {'fixture_id':FIXTURE_ID,'expected':EXPECTED,'phases':{}}
    report['phases'][phase] = payload
    report['acquisition_verified'] = False
    report['full_bootstrap_verified'] = False
    write_owned_json(path,report)


def files_check(rerun):
    snapshot = files_snapshot()
    path = REPORTS/'p63_files_baseline.json'
    if path.exists():
        equal(snapshot,json.loads(path.read_text()),'Rerun must preserve all output batches/hash/mtime')
    else:
        require(not rerun, 'First file baseline missing')
        write_owned_json(path,snapshot)
    update_report('files-rerun' if rerun else 'files-first',
                  {'status':'pass','taxi_bronze':30,'taxi_silver':20,'taxi_gold':20,
                   'rejected':10,'physical_files':len(snapshot['physical_files']),
                   'batch_ids_hashes_mtimes_unchanged':bool(rerun)})
    print(json.dumps({'files':'pass','rerun':rerun,'bronze':30,'silver':20,'gold':20,'rejected':10}))


def database_check(rerun):
    snapshot = database_snapshot(require_complete=True)
    report = json.loads((REPORTS/'postgres_load_report.json').read_text())
    require(report['validation_status']=='valid','Real loader validation did not pass')
    equal(report['dimension_counts'],{'location':265,'calendar':365,'weather':4344},'Real validation dimensions')
    equal(report['per_month_counts'],COUNTS,'Real validation monthly counts')
    require(report['calendar_holiday_count']==11 and report['fact_summary']['total_trips']==20
            and report['fact_summary']['calendar_null_fk']==1 and report['fact_summary']['weather_null_fk']==2,
            'Real validation independent totals/FKs')
    require(INDEXES.issubset(report['indexes']) and report['view_trip_enriched_present'], 'Indexes/view missing')
    facts = snapshot['facts']
    metrics = {'fare_sum':sum(r['fare_amount'] for r in facts),
               'fare_average':sum(r['fare_amount'] for r in facts)/20,
               'distance_sum':sum(r['trip_distance'] for r in facts),
               'distance_average':sum(r['trip_distance'] for r in facts)/20,
               'negative_fare':sum(r['fare_amount']<0 for r in facts),
               'zero_fare':sum(r['fare_amount']==0 for r in facts),
               'null_passengers':sum(r['passenger_count'] is None for r in facts)}
    equal(metrics,{k:EXPECTED[k] for k in metrics},'Independent actual fact KPI totals')
    # Report sanity SQL is evaluated by the real loader, not substituted with log counts.
    sanity = report['sanity_queries']
    equal(sorted(sanity['by_pickup_borough'],key=canonical),[{'borough':'Fixture Manhattan','trips':20}],'Borough join aggregate')
    equal(sanity['by_pickup_hour'],[{'pickup_hour':10,'trips':6},{'pickup_hour':11,'trips':6},
                                  {'pickup_hour':12,'trips':7},{'pickup_hour':23,'trips':1}],'Pickup-hour aggregate')
    equal(sanity['by_holiday'],[{'is_holiday':False,'trips':13},{'is_holiday':True,'trips':6},
                              {'is_holiday':None,'trips':1}],'Holiday/NULL calendar aggregate')
    equal(sanity['by_weather_match'],[{'weather_matched':False,'trips':2},
                                    {'weather_matched':True,'trips':18}],'Weather matched aggregate')
    equal(sorted(sanity['by_rain'],key=lambda r:r['weather_class']),
          [{'weather_class':'dry','trips':17},{'weather_class':'rainy','trips':1},{'weather_class':'unknown','trips':2}], 'Weather rain aggregate')
    path = REPORTS/'p63_database_baseline.json'
    if path.exists():
        equal(snapshot,json.loads(path.read_text()),'Rerun trip IDs/weather IDs/log timestamps/content must not change')
    else:
        require(not rerun,'First database baseline missing')
        write_owned_json(path,snapshot)
    update_report('database-rerun' if rerun else 'database-first',
                  {'status':'pass','actual_counts':snapshot['counts'],'metrics':metrics,
                   'calendar_null_fk':1,'weather_null_fk':2,'validation_status':report['validation_status'],
                   'trip_ids_weather_ids_log_timestamps_unchanged':bool(rerun)})
    print(json.dumps({'database':'pass','rerun':rerun,'counts':snapshot['counts'],'metrics':metrics,'validation':'valid'}))


class HarnessTests(unittest.TestCase):
    @staticmethod
    def owned_database_records():
        gold = {f'taxi_{month}':{'gold_batch_id':'gold-'+month,
                'source_silver_batch_id':'silver-'+month,
                'source_lineage':{'bronze_batch_id':'bronze-'+month}} for month in MONTHS}
        facts = [{**expected_fact(case,gold),'trip_id':index,'weather_id':index if case[5] else None,
                  'weather_timestamp_utc':expected_gold(case)['weather_timestamp_utc']}
                 for index,case in enumerate(CASES,1)]
        facts.sort(key=lambda r:(r['source_month'],r['pickup_datetime_local']))
        logs = [{'dataset':'fact_trip','source_year':2025,'source_month':month,
                 'gold_batch_id':'gold-'+label,'source_row_count':COUNTS[label],
                 'loaded_row_count':COUNTS[label],'started_at':'present','completed_at':'present',
                 'status':'loaded'} for month,label in enumerate(MONTHS,1)]
        return gold,{'facts':facts,'logs':logs}

    def test_foreign_fact_content_rejected_even_at_expected_count(self):
        gold,snapshot = self.owned_database_records()
        with patch(__name__+'.manifests',return_value=gold):
            assert_fact_ownership(snapshot,True)
            snapshot['facts'][0]['fare_amount']=999.
            with self.assertRaises(FixtureError):
                assert_fact_ownership(snapshot,True)

    def test_stale_log_rejected_before_loader_can_replace_partition(self):
        gold,snapshot = self.owned_database_records()
        with patch(__name__+'.manifests',return_value=gold):
            assert_fact_ownership(snapshot,True)
            snapshot['logs'][0]['gold_batch_id']='foreign-batch'
            with self.assertRaises(FixtureError):
                assert_fact_ownership(snapshot,True)

    def test_literal_expected_totals(self):
        self.assertEqual(len(CASES),20)
        self.assertEqual(sum(c[2] for c in CASES),80.)
        self.assertEqual(sum(c[3] for c in CASES),30.)
        self.assertEqual(sum(len(raw_rows(m)) for m in range(1,7)),30)

    def test_raw_month_cardinalities(self):
        self.assertEqual({f'2025-{m:02d}':len(raw_rows(m)) for m in range(1,7)},RAW_COUNTS)

    def test_negative_zero_null_preserved_spec(self):
        self.assertEqual(sum(c[2]<0 for c in CASES),6)
        self.assertEqual(sum(c[2]==0 for c in CASES),6)
        self.assertEqual(sum(c[4] is None for c in CASES),6)

    def test_dst_oracle_literal_checkpoints(self):
        self.assertEqual(expected_weather(datetime(2025,3,9,6,tzinfo=timezone.utc))['weather_hour_nyc'],1)
        self.assertEqual(expected_weather(datetime(2025,3,9,7,tzinfo=timezone.utc))['weather_hour_nyc'],3)

    def test_explicit_missing_joins(self):
        self.assertEqual(sum(not expected_gold(c)['calendar_matched'] for c in CASES),1)
        self.assertEqual(sum(not expected_gold(c)['weather_matched'] for c in CASES),2)

    def test_gold_does_not_project_weather_join_helper_columns(self):
        self.assertNotIn('weather_date_nyc',expected_gold(CASES[0]))
        self.assertNotIn('weather_hour_nyc',expected_gold(CASES[0]))
        self.assertIn('weather_date_nyc',expected_weather(datetime(2025,1,1,tzinfo=timezone.utc)))

    def test_empty_database_allowed(self):
        permit_counts(dict.fromkeys(TABLE_LIMITS,0),False)

    def test_foreign_database_rejected(self):
        with self.assertRaises(FixtureError):
            permit_counts(dict(TABLE_LIMITS),False)

    def test_excess_rows_rejected(self):
        with self.assertRaises(FixtureError):
            permit_counts({**TABLE_LIMITS,'fact_trip':21},True)

    def test_partial_foreign_dimensions_rejected(self):
        with self.assertRaises(FixtureError):
            permit_counts({**TABLE_LIMITS,'dim_location':264},True)

    def test_comparison_rejects_wrong_null(self):
        with self.assertRaises(FixtureError):
            equal({'passenger_count':0},{'passenger_count':None},'NULL contract')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('self-test','guard','prepare','files-first','files-rerun','database-first','database-rerun'))
    action = parser.parse_args().action
    if action == 'self-test':
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(HarnessTests)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    require(Path.cwd()==ROOT and os.environ.get('P63_EXPECTED_IMAGE_ID')==VERIFIED_IMAGE,
            'Run only through the isolated P6.3 orchestrator with the verified clean image')
    require(all((ROOT/d).is_dir() for d in ('data','logs','models','reports')),'Fixture runtime mounts missing')
    if action == 'guard':
        guard()
    elif action == 'prepare':
        prepare()
    elif action.startswith('files-'):
        files_check(action.endswith('rerun'))
    else:
        database_check(action.endswith('rerun'))
    return 0


if __name__=='__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        detail = str(exc) if isinstance(exc,FixtureError) else 'Unexpected failure; raw details withheld'
        line = traceback.extract_tb(exc.__traceback__)[-1].lineno
        print(json.dumps({'fixture_error':type(exc).__name__,'detail':detail,'line':line}),file=sys.stderr)
        sys.exit(1)
