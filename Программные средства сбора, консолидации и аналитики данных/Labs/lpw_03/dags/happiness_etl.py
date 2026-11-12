"""World Happiness ETL. Pure transformations can also be checked without Airflow."""
import csv
import hashlib
import json
import logging
import math
import os
import re
import shutil
from pathlib import Path

YEARS = range(2015, 2020)
DATASET = 'unsdsn/world-happiness'
ROOT = Path(__file__).resolve().parent
WORK = ROOT / 'data'
ALIASES = {
    'Taiwan Province of China': 'Taiwan',
    'Hong Kong S.A.R., China': 'Hong Kong',
    'Northern Cyprus': 'North Cyprus',
    'North Macedonia': 'Macedonia',
    'Trinidad & Tobago': 'Trinidad and Tobago',
}
FIELDS = {
    2015: ('Country', 'Happiness Score', 'Economy (GDP per Capita)', 'Family'),
    2016: ('Country', 'Happiness Score', 'Economy (GDP per Capita)', 'Family'),
    2017: ('Country', 'Happiness.Score', 'Economy..GDP.per.Capita.', 'Family'),
    2018: ('Country or region', 'Score', 'GDP per capita', 'Social support'),
    2019: ('Country or region', 'Score', 'GDP per capita', 'Social support'),
}


def normalize(value):
    return re.sub(r'\s+', ' ', value or '').strip()


def country_name(value):
    name = normalize(value)
    return ALIASES.get(name, name)


def extract(source=None):
    """No silent local fallback: the assessed path always calls Kaggle."""
    source = source or os.environ.get('DATA_SOURCE', 'kaggle')
    WORK.mkdir(parents=True, exist_ok=True)
    if source == 'kaggle':
        import kagglehub
        source_dir = Path(kagglehub.dataset_download(DATASET))
    elif source == 'local':
        source_dir = Path(os.environ.get('LOCAL_DATA_DIR', '/opt/airflow/input'))
    else:
        raise ValueError('DATA_SOURCE must be kaggle or local')
    manifest = {'source': source, 'dataset': DATASET, 'files': []}
    snapshot = WORK / 'source'
    snapshot.mkdir(exist_ok=True)
    for year in YEARS:
        matches = list(source_dir.rglob('%s.csv' % year))
        if len(matches) != 1:
            raise ValueError('Expected exactly one %s.csv, found %s' % (year, len(matches)))
        target = snapshot / ('%s.csv' % year)
        shutil.copyfile(matches[0], target)
        manifest['files'].append({
            'name': target.name, 'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
        })
    (WORK / 'source_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    return manifest


def read_sources(directory):
    result, headers = [], []
    for year in YEARS:
        with (Path(directory) / ('%s.csv' % year)).open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            required = set(FIELDS[year]) | ({'Region'} if year <= 2016 else set())
            if not required.issubset(reader.fieldnames or []):
                raise ValueError('Unexpected CSV schema for %s' % year)
            for header in reader.fieldnames:
                if header not in headers:
                    headers.append(header)
            for number, row in enumerate(reader, 1):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError('Malformed CSV record: %s, %s' % (year, number))
                # Original cell strings are not stripped or converted in RAW.
                result.append({'source_year': str(year), 'source_file': '%s.csv' % year,
                               'source_row': str(number), **row})
    return result, headers


def number(value):
    if normalize(value) in ('', 'NA', 'N/A', 'NaN', 'nan', 'null'):
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('non-finite number')
    return result


def transform(rows):
    """One accepted observation per canonical country/year; audit every rejection."""
    regions = {}
    # Prefer latest available region classification, and retain its provenance.
    for row in sorted(rows, key=lambda r: int(r['source_year'])):
        region = normalize(row.get('Region'))
        if region:
            name = country_name(row.get('Country'))
            regions[name] = (region, row['source_file'])

    accepted, audit, seen = [], [], set()
    for row in rows:
        reasons = []
        year = int(row['source_year'])
        fields = FIELDS[year]
        country = country_name(row.get(fields[0]))
        region = normalize(row.get('Region'))
        region_source = row['source_file'] if region else regions.get(country, (None, None))[1]
        region = region or regions.get(country, ('Unknown', None))[0]
        if not country:
            reasons.append('empty_country')
        values = []
        for field in fields[1:]:
            try:
                values.append(number(row.get(field)))
            except (ValueError, TypeError):
                values.append(None)
                reasons.append('invalid_numeric:' + field)
        score, gdp, support = values
        if score is None or not 0 <= score <= 10:
            reasons.append('missing_or_out_of_range_score')
        if gdp is not None and gdp < 0:
            reasons.append('negative_gdp_component')
        if support is not None and support < 0:
            reasons.append('negative_social_support_component')
        key = (country, year)
        if key in seen:
            reasons.append('duplicate_country_year')
        if reasons:
            audit.append({'source_file': row['source_file'], 'source_row': row['source_row'],
                          'action': 'rejected', 'reason': ';'.join(reasons), 'payload': row})
            continue
        seen.add(key)
        accepted.append({
            'country': country, 'region': region, 'year': year,
            'happiness_score': score, 'gdp_per_capita': gdp, 'social_support': support,
            'region_source': region_source,
            'source_file': row['source_file'], 'source_row': row['source_row'],
        })
        warnings = []
        if region == 'Unknown':
            warnings.append('region_not_available')
        if gdp is None:
            warnings.append('missing_gdp_kept_null')
        if support is None:
            warnings.append('missing_support_kept_null')
        if warnings:
            audit.append({'source_file': row['source_file'], 'source_row': row['source_row'],
                          'action': 'warning', 'reason': ';'.join(warnings), 'payload': row})
    assert len(rows) == len(accepted) + sum(a['action'] == 'rejected' for a in audit)
    return accepted, audit


def connection():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id='analytics_postgres').get_conn()


def load_raw():
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    rows, headers = read_sources(WORK / 'source')
    columns = ['source_file', 'source_row', 'source_year'] + headers
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS raw_happiness ({})').format(
                    sql.SQL(', ').join(sql.SQL('{} TEXT').format(sql.Identifier(c)) for c in columns)))
                # New source columns may be added without discarding prior schema.
                for col in columns:
                    cur.execute(sql.SQL('ALTER TABLE raw_happiness ADD COLUMN IF NOT EXISTS {} TEXT')
                                .format(sql.Identifier(col)))
                cur.execute('TRUNCATE raw_happiness')
                query = sql.SQL('INSERT INTO raw_happiness ({}) VALUES %s').format(
                    sql.SQL(', ').join(map(sql.Identifier, columns)))
                execute_values(cur, query.as_string(cur),
                               [[row.get(col) for col in columns] for row in rows])
        logging.info('RAW: %s records, all source columns TEXT', len(rows))
        return len(rows)
    finally:
        conn.close()


def load_staging(run_id):
    from psycopg2.extras import RealDictCursor, execute_values, Json
    conn = connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('SELECT * FROM raw_happiness ORDER BY source_year, source_row::int')
                rows = [dict(r) for r in cur.fetchall()]
            accepted, audit = transform(rows)
            with conn.cursor() as cur:
                cur.execute((ROOT / 'schema.sql').read_text(encoding='utf-8'))
                cur.execute('TRUNCATE stg_happiness')
                columns = list(accepted[0]) if accepted else []
                if accepted:
                    execute_values(cur, 'INSERT INTO stg_happiness (' + ','.join(columns) + ') VALUES %s',
                                   [[r[c] for c in columns] for r in accepted])
                # A retry replaces this run's audit rather than duplicating it.
                cur.execute('DELETE FROM etl_quality_log WHERE run_id=%s', (run_id,))
                if audit:
                    execute_values(cur, '''INSERT INTO etl_quality_log
                        (run_id,source_file,source_row,action,reason,payload) VALUES %s''',
                        [(run_id, a['source_file'], int(a['source_row']), a['action'],
                          a['reason'], Json(a['payload'])) for a in audit])
                cur.execute('''INSERT INTO etl_runs (run_id,raw_count,stg_count,rejected_count,warning_count)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT (run_id) DO UPDATE SET
                    raw_count=EXCLUDED.raw_count,stg_count=EXCLUDED.stg_count,
                    rejected_count=EXCLUDED.rejected_count,warning_count=EXCLUDED.warning_count,
                    loaded_at=NOW()''',
                    (run_id, len(rows), len(accepted), sum(a['action'] == 'rejected' for a in audit),
                     sum(a['action'] == 'warning' for a in audit)))
        logging.info('RAW=%s STG=%s rejected=%s warnings=%s', len(rows), len(accepted),
                     sum(a['action'] == 'rejected' for a in audit),
                     sum(a['action'] == 'warning' for a in audit))
        return len(accepted)
    finally:
        conn.close()


def create_datamart():
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute((ROOT / 'datamart_variant_09.sql').read_text(encoding='utf-8'))
    finally:
        conn.close()


def validate_datamart(run_id):
    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM happiness_datamart')
            count = cur.fetchone()[0]
            cur.execute('SELECT raw_count,stg_count,rejected_count FROM etl_runs WHERE run_id=%s', (run_id,))
            raw, stg, rejected = cur.fetchone()
            if not (count == stg and raw == stg + rejected and count > 0):
                raise ValueError('Layer counts do not reconcile')
            cur.execute('''SELECT COUNT(*) FROM happiness_datamart WHERE country IS NULL
                OR region IS NULL OR year NOT BETWEEN 2015 AND 2019
                OR happiness_score NOT BETWEEN 0 AND 10''')
            if cur.fetchone()[0]:
                raise ValueError('Invalid values in datamart')
            cur.execute('SELECT year,COUNT(*),AVG(happiness_score) FROM happiness_datamart GROUP BY year ORDER BY year')
            logging.info('Datamart verified: %s', cur.fetchall())
    finally:
        conn.close()
