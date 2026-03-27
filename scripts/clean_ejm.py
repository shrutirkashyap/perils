import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / 'data' / '01_raw' / 'EJM.csv'
CLEAN_DIR = ROOT / 'data' / '02_cleaned'
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

RECORDS_PATH = CLEAN_DIR / 'ejm_records.csv'
JURIS_PATH = CLEAN_DIR / 'ejm_jurisprudence_types.csv'
ACTOR_PATH = CLEAN_DIR / 'ejm_actor_types.csv'
SUMMARY_PATH = CLEAN_DIR / 'ejm_summary.json'

FLAG_MAP = {
    'ron': 'RoN',
    'personhood': 'Personhood',
    'animal_rights': 'AnimalRights',
    'lek': 'LEK',
    'indigenous_framing': 'Indigenous',
    'eco_gov': 'EcoGov',
    'actor_indigenous': 'Indigenous.1',
    'actor_ngo': 'NGO',
    'actor_civsoc': 'CivSoc',
    'actor_international': 'International',
    'actor_government': 'Government',
    'actor_court': 'Court',
}


def clean_text(value: str) -> str:
    if value is None:
        return ''
    return ' '.join(str(value).replace('\ufeff', '').split())


def split_pipe(value: str) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    return [part.strip() for part in text.split('|') if part.strip()]


def parse_int(value: str):
    text = clean_text(value)
    if not text:
        return ''
    try:
        return int(float(text))
    except ValueError:
        return ''


def parse_float(value: str):
    text = clean_text(value)
    if not text:
        return ''
    try:
        return round(float(text), 6)
    except ValueError:
        return ''


def bool_flag(value: str) -> int:
    text = clean_text(value)
    return 1 if text == '1' else 0


def status_parts(row: dict) -> tuple[str, str]:
    parts = []
    latest = ''
    for idx in range(1, 6):
        primary = clean_text(row.get(f'Status_{idx}_Primary', ''))
        year = clean_text(row.get(f'Status_{idx}_Year', ''))
        secondary = clean_text(row.get(f'Status_{idx}_Secondary', ''))
        if not primary:
            continue
        latest = primary
        piece = primary
        if year:
            piece += f' ({year})'
        if secondary:
            piece += f' - {secondary}'
        parts.append(piece)
    return latest or 'Unspecified', ' | '.join(parts)


records = []
exploded_juris = []
exploded_actor_types = []
summary = {
    'rows': 0,
    'regions': set(),
    'countries': set(),
    'min_year': None,
    'max_year': None,
}

with RAW_PATH.open('r', encoding='utf-8-sig', newline='') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        record_id = parse_int(row.get('ID', ''))
        country_tokens = split_pipe(row.get('Country', ''))
        country_code = country_tokens[0] if country_tokens else ''
        country_name = country_tokens[-1] if country_tokens else clean_text(row.get('Country', ''))
        region = clean_text(row.get('Region', '')) or 'Unspecified'
        legal_provision = clean_text(row.get('Type of Legal Provision', '')) or 'Unspecified'
        year_initiated = parse_int(row.get('Year Initiated', ''))
        lat = parse_float(row.get('Latitude', ''))
        lon = parse_float(row.get('Longitude', ''))
        jurisprudence_types = split_pipe(row.get('Type of Ecological Jurisprudence', ''))
        actor_types = split_pipe(row.get('Initiating Actor Type', ''))
        status_current, status_history = status_parts(row)

        cleaned = {
            'record_id': record_id,
            'title': clean_text(row.get('Title', '')),
            'lat': lat,
            'lon': lon,
            'country_code': country_code,
            'country_name': country_name or 'Unspecified',
            'country_raw': clean_text(row.get('Country', '')),
            'location': clean_text(row.get('Location', '')),
            'region': region,
            'legal_provision': legal_provision,
            'status_current': status_current,
            'status_history': status_history,
            'year_initiated': year_initiated,
            'jurisprudence_types': ' | '.join(jurisprudence_types),
            'jurisprudence_type_primary': jurisprudence_types[0] if jurisprudence_types else 'Unspecified',
            'initiating_actor_name': clean_text(row.get('Initiating Actor Name', '')),
            'initiating_actor_types': ' | '.join(actor_types),
            'initiating_actor_type_primary': actor_types[0] if actor_types else 'Unspecified',
            'ecological_actor_name': clean_text(row.get('Ecological Actor Name', '')),
            'ecological_actors': clean_text(row.get('Ecological Actors', '')),
            'theoretical_framing': clean_text(row.get('Theoretical Framing', '')),
            'identified_ecosystem_flag': bool_flag(row.get('Identified Ecosystem', '')),
            'caretaking_system_code': parse_int(row.get('Caretaking System', '')),
            'caretaking_actor_code': parse_int(row.get('Caretaking Actor', '')),
            'representation_code': clean_text(row.get('Representation', '')),
            'synopsis': clean_text(row.get('Synopsis', '')),
            'permalink': clean_text(row.get('Permalink', '')),
        }

        for output_name, source_name in FLAG_MAP.items():
            cleaned[output_name] = bool_flag(row.get(source_name, ''))

        records.append(cleaned)
        summary['rows'] += 1
        summary['regions'].add(region)
        summary['countries'].add(cleaned['country_name'])
        if isinstance(year_initiated, int):
            summary['min_year'] = year_initiated if summary['min_year'] is None else min(summary['min_year'], year_initiated)
            summary['max_year'] = year_initiated if summary['max_year'] is None else max(summary['max_year'], year_initiated)

        for value in jurisprudence_types or ['Unspecified']:
            exploded_juris.append({'record_id': record_id, 'jurisprudence_type': value})
        for value in actor_types or ['Unspecified']:
            exploded_actor_types.append({'record_id': record_id, 'actor_type': value})

record_columns = [
    'record_id', 'title', 'lat', 'lon', 'country_code', 'country_name', 'country_raw', 'location',
    'region', 'legal_provision', 'status_current', 'status_history', 'year_initiated',
    'jurisprudence_types', 'jurisprudence_type_primary', 'initiating_actor_name',
    'initiating_actor_types', 'initiating_actor_type_primary', 'ecological_actor_name',
    'ecological_actors', 'theoretical_framing', 'ron', 'personhood', 'animal_rights', 'lek',
    'indigenous_framing', 'eco_gov', 'actor_indigenous', 'actor_ngo', 'actor_civsoc',
    'actor_international', 'actor_government', 'actor_court', 'identified_ecosystem_flag',
    'caretaking_system_code', 'caretaking_actor_code', 'representation_code', 'synopsis', 'permalink'
]

with RECORDS_PATH.open('w', encoding='utf-8', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=record_columns)
    writer.writeheader()
    writer.writerows(records)

with JURIS_PATH.open('w', encoding='utf-8', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['record_id', 'jurisprudence_type'])
    writer.writeheader()
    writer.writerows(exploded_juris)

with ACTOR_PATH.open('w', encoding='utf-8', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['record_id', 'actor_type'])
    writer.writeheader()
    writer.writerows(exploded_actor_types)

summary_output = {
    'rows': summary['rows'],
    'regions': sorted(summary['regions']),
    'countries': len(summary['countries']),
    'min_year': summary['min_year'],
    'max_year': summary['max_year'],
}
SUMMARY_PATH.write_text(json.dumps(summary_output, indent=2), encoding='utf-8')

print(f'Wrote {RECORDS_PATH}')
print(f'Wrote {JURIS_PATH}')
print(f'Wrote {ACTOR_PATH}')
print(f'Wrote {SUMMARY_PATH}')
