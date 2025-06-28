import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from datetime import datetime
# Define constants at the top
PROJECT_ID = '<Project_id>'
BUCKET_NAME = '<bucket_name>'
TOPIC = f'projects/{PROJECT_ID}/topics/new-orders'
BQ_DATASET = 'instacart_scoring'
BQ_TABLE = 'reorder_predictions'
BQ_MODEL = f'{BQ_DATASET}.reorder_model'
class PredictReorderFn(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        record['event_time'] = record['event_time']
        # Simulated prediction query (replace with actual BigQuery API call)
        score = 0.75 # placeholder
        return [{
        'user_id': record['user_id'],
        'product_id': record['product_id'],
        'score': score,
        'event_time': record['event_time'],
        'prediction_time': datetime.utcnow().isoformat()
    }]
def run():
    options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    region='us-east1',
    temp_location=f'gs://{BUCKET_NAME}/temp',
    runner='DataflowRunner'
    )
    with beam.Pipeline(options=options) as p:
        (p
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=TOPIC)
        | 'PredictReorder' >> beam.ParDo(PredictReorderFn())
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=BQ_TABLE,
            dataset=BQ_DATASET,
            project=PROJECT_ID,
            schema='user_id:INTEGER, product_id:INTEGER, score:FLOAT,event_time:TIMESTAMP, prediction_time:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
if __name__ == '__main__':
    run()

