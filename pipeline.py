"""Educational batch ingestion of synthetic bookings; Python standard library."""
import argparse
import csv
import datetime as dt
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import sqlite3
import uuid


def validate(row):
    if not isinstance(row, dict):
        raise ValueError('record_must_be_object')
    booking_id = row.get('booking_id')
    if not isinstance(booking_id, str) or not re.fullmatch(r'[A-Z0-9_-]{1,40}', booking_id):
        raise ValueError('invalid_booking_id')
    for field in ('origin', 'destination'):
        if not isinstance(row.get(field), str) or not re.fullmatch('[A-Z]{3}', row[field]):
            raise ValueError('invalid_' + field)
    if row['origin'] == row['destination']:
        raise ValueError('same_origin_and_destination')
    if row.get('status') not in ('confirmed', 'cancelled'):
        raise ValueError('invalid_status')
    if row.get('currency') not in ('RUB', 'USD', 'EUR'):
        raise ValueError('invalid_currency')
    try:
        price = Decimal(str(row['price']))
        if not price.is_finite() or price < 0 or price > Decimal('100000000'):
            raise ValueError('invalid_price')
        minor = price * 100
        if minor != minor.to_integral_value():
            raise ValueError('too_many_decimal_places')
    except (InvalidOperation, KeyError):
        raise ValueError('invalid_price') from None
    try:
        booked = dt.date.fromisoformat(row['booked_at'])
        departure = dt.date.fromisoformat(row['departure_date'])
        updated = dt.datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
        if updated.tzinfo is None:
            raise ValueError('updated_at_requires_timezone')
        updated = updated.astimezone(dt.timezone.utc)
    except (KeyError, TypeError, AttributeError, ValueError):
        raise ValueError('invalid_date_or_timezone') from None
    if departure < booked or updated.date() < booked:
        raise ValueError('inconsistent_dates')
    return (booking_id, row['origin'], row['destination'], row['status'], row['currency'],
            int(minor), booked.isoformat(), departure.isoformat(), updated.isoformat())


def run(source, database, output):
    output.mkdir(parents=True, exist_ok=True)
    database.parent.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    counts = {'received': 0, 'inserted': 0, 'updated': 0, 'unchanged': 0, 'rejected': 0}
    rejected = []
    connection = sqlite3.connect(database)
    connection.execute('''CREATE TABLE IF NOT EXISTS bookings (
        booking_id TEXT PRIMARY KEY, origin TEXT NOT NULL, destination TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('confirmed','cancelled')),
        currency TEXT NOT NULL, price_minor INTEGER NOT NULL CHECK(price_minor>=0),
        booked_at TEXT NOT NULL, departure_date TEXT NOT NULL, updated_at TEXT NOT NULL)''')
    connection.execute('''CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id TEXT PRIMARY KEY, source TEXT, finished_at TEXT, received INTEGER,
        inserted INTEGER, updated INTEGER, unchanged INTEGER, rejected INTEGER)''')
    with connection:
        with source.open(encoding='utf-8-sig') as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                counts['received'] += 1
                try:
                    row = json.loads(line)
                    values = validate(row)
                    previous = connection.execute('SELECT updated_at FROM bookings WHERE booking_id=?', (values[0],)).fetchone()
                    if previous and previous[0] >= values[-1]:
                        counts['unchanged'] += 1
                        continue
                    connection.execute('''INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(booking_id) DO UPDATE SET origin=excluded.origin,
                        destination=excluded.destination,status=excluded.status,
                        currency=excluded.currency,price_minor=excluded.price_minor,
                        booked_at=excluded.booked_at,departure_date=excluded.departure_date,
                        updated_at=excluded.updated_at WHERE excluded.updated_at>bookings.updated_at''', values)
                    counts['updated' if previous else 'inserted'] += 1
                except (ValueError, TypeError) as exc:
                    counts['rejected'] += 1
                    rejected.append({'line': line_number, 'reason': str(exc), 'raw': line.strip()})
        connection.execute('INSERT INTO ingestion_runs VALUES (?,?,?,?,?,?,?,?)',
                           (run_id, source.name, dt.datetime.now(dt.timezone.utc).isoformat(),
                            counts['received'], counts['inserted'], counts['updated'], counts['unchanged'], counts['rejected']))
    sql = (Path(__file__).parent / 'report.sql').read_text(encoding='utf-8')
    cursor = connection.execute(sql)
    rows = cursor.fetchall()
    with (output / 'route_report.csv').open('w', encoding='utf-8-sig', newline='') as report:
        writer = csv.writer(report)
        writer.writerow([column[0] for column in cursor.description])
        writer.writerows(rows)
    (output / ('quarantine_' + run_id + '.jsonl')).write_text(
        '\n'.join(json.dumps(r, ensure_ascii=False) for r in rejected), encoding='utf-8')
    summary = {'run_id': run_id, 'source': source.name, **counts,
               'total_bookings': connection.execute('SELECT count(*) FROM bookings').fetchone()[0]}
    (output / ('run_' + run_id + '.json')).write_text(json.dumps(summary, indent=2), encoding='utf-8')
    connection.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('--database', type=Path, default=Path('result/bookings.sqlite3'))
    parser.add_argument('--output', type=Path, default=Path('result'))
    args = parser.parse_args()
    run(args.source, args.database, args.output)
