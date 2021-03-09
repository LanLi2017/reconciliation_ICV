import csv
import json
from operator import itemgetter
from pprint import pprint

import requests

from refine_pkg.OpenRefineClientPy3.google_refine.refine import refine


# temporal column status induce
#

def connect_service():
    pass


def req_recon_api(rec_api='https://wikidata.reconci.link/en/api', data=None, query_type=None, limit=3, batch_size =2):
    '''
    http://localhost:8000/reconcile
    :param limit: an integer to specify how many results to return.
    :param query_type: a single string, or an array of strings, specifying the types of result. e.g. person, product...
    :param rec_api: reconciliation service address
    :param data: data needs to be reconciled
    :param batch_size: how many rows per request sent to the service
    :return: query results
    '''
    for i in range(0, len(data), batch_size):
        query_batch = data[i:i + batch_size]
        queries = {
            query: {
                'query': query,
                # Some optional parameter, e.g.
                'limit': limit,
                'type': query_type,
            }
            for query in query_batch
        }
        response = requests.get(rec_api, params={'queries': json.dumps(queries)})
        response.raise_for_status()
        response_data = response.json()
        for query in query_batch:
            results = response_data[query]['result']
            results.sort(key=itemgetter('score'), reverse=True)
            for result in results:
                # Use pprint(result) to check some other info
                # id; name; type;
                # score: double
                # match: boolean, true if the service is quite confident about the result
                print(f'Query: {query}\t Result Value: {result["name"]}\t Score: {result["score"]}')


def main():
    # project_id = 2478996104406
    project_id = 2486157629041  # 723 rows
    op = refine.RefineProject(refine.RefineServer(), project_id)

    # export [for further applied]
    # for row in refine.RefineProject(refine.RefineServer(),project_id).export():
    #     ds_writer.writerow(row)

    # export current dataset
    # with open('data.csv', 'wb')as f:
    #     f.write(op.export(export_format='csv').content)

    # reconciliation
    # recon_types = op.guess_types_of_column('City', 'http://localhost:8000/reconcile')
    # print(recon_types)

    # op.reconcile(column='City', service='http://localhost:8000/reconcile', reconciliation_type=recon_types[0],
    #              relevant_column='Zip', property_name='ZIP', property_id='ZIP')
    # res = op.get_reconcile_result()
    # pprint(res)
    response = op.get_models()
    column_model = response['columnModel']
    pprint(column_model)
    column_index = {}  # map of column name to index into get_rows() data
    columns = [column['name'] for column in column_model['columns']]

    column_order = {}
    for i, column in enumerate(column_model['columns']):
        name = column['name']
        column_order[name] = i
        column_index[name] = column['cellIndex']
    key_column = column_model['keyColumnName']
    has_records = response['recordModel'].get('hasRecords', False)

    history = op.list_history()
    pprint(history)


def recon_main():

    pass


if __name__ == "__main__":
    recon_main()
    # main()
