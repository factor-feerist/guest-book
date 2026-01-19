import ydb
import urllib.parse
import hashlib
import base64
import json
import os

BACKEND_VERSION = '1.1.4'

def decode(event, body):
    is_base64_encoded = event.get('isBase64Encoded')
    if is_base64_encoded:
        body = str(base64.b64decode(body), 'utf-8')
    return json.loads(body)

def response(statusCode, headers, isBase64Encoded, body):
    return {
        'statusCode': statusCode,
        'headers': headers,
        'isBase64Encoded': isBase64Encoded,
        'body': body,
    }

def get_config():
    endpoint = os.getenv("endpoint")
    database = os.getenv("database")
    if endpoint is None or database is None:
        raise AssertionError("Нужно указать обе переменные окружения")
    credentials = ydb.construct_credentials_from_environ()
    return ydb.DriverConfig(endpoint, database, credentials=credentials)


def execute(config, query, params):
    with ydb.Driver(config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            return None

        session = driver.table_client.session().create()
        prepared_query = session.prepare(query)

        return session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query,
            params,
            commit_tx=True
        )

def insert_guest(name, message):
    config = get_config()
    query = """
        DECLARE $name AS Utf8;
        DECLARE $message AS Utf8;

        UPSERT INTO guests (name, message) VALUES ($name, $message);
        """
    params = {'$name': name, '$message': message}
    execute(config, query, params)

def register(event):
    body = event.get('body')

    if body:
        data_dict = decode(event, body)
        insert_guest(data_dict['name'], data_dict['message'])
        return response(200, {}, False, '')

    return response(400, {}, False, 'No name or message')

def get_version():
    return response(200, {}, False, json.dumps({'version': f'{BACKEND_VERSION}'}))

def get_guests():
    print(id)
    config = get_config()
    query = """
        SELECT * FROM guests;
        """
    params = {}
    result_set = execute(config, query, {})
    if not result_set or not result_set[0].rows:
        return None

    guests = []
    for row in result_set[0].rows:
        guests.append({
            "name": row.name.decode('utf-8') if isinstance(row.name, bytes) else row.name,
            "message": row.message.decode('utf-8') if isinstance(row.message, bytes) else row.message
        })

    return response(200, {}, False, json.dumps({'guests': f'{guests}'}))

def get_result(url, event):
    if url == "/register":
        return register(event)
    if url == "/guests":
        return get_guests()
    if url == "/backend-version":
        return get_version()

    return response(404, {}, False, 'No such path')


def handler(event, context):
    url = event.get('url')
    if url:
        if url[-1] == '?':
            url = url[:-1]
        return get_result(url, event)

    return response(404, {}, False, 'Not found')
