import requests
from google.cloud import bigquery
import time

def getRow():
    response = requests.get('http://api.open-notify.org/iss-now.json')
    data = response.json()
    # row_to_insert = [float(data['iss_position']['latitude']), float(data['iss_position']['longitude']), data['message'], data['timestamp']]
    row_to_insert = [float(data['iss_position']['latitude']), float(data['iss_position']['longitude']),'ok', data['timestamp']]
    return row_to_insert

client = bigquery.Client()
dataset = client.get_dataset('ensemble_donnees_1')
table = client.get_table(dataset.table('table_allo_2'))

    
while True:
    row_to_insert = getRow()
    errors = client.insert_rows(table, [row_to_insert])
    if errors:
        print(f'Encountered errors while inserting rows: {errors}')
    else:
        print(str(row_to_insert[0]) + '   \t' + str(row_to_insert[1]) + '   \t' + str(row_to_insert[2]) + '\t' + str(row_to_insert[3]))
    time.sleep(1)
