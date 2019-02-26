import csv
import uuid
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper


class ProcessCSV(beam.DoFn):
    def process(self, element, headers):
        try:
            for line in csv.reader([element]):
                if len(line) == len(headers):
                    data = {header.strip(): val.strip() for header, val in zip(headers, line)}
                    return [data]

                else:
                    logging.info("row contains bad data")
        except:
            logging.exception('')
            pass

class BuildEntities(beam.DoFn):
    def process(self, element, entityName, user, dataset):
        entity = entity_pb2.Entity()
        datastore_helper.add_key_path(entity.key, entityName.get(), str(uuid.uuid4()))

        datastore_helper.add_properties(entity,
          {
            "label": unicode(element['label']),
            "user": unicode(user.get()),
            "dataset": unicode(dataset.get())
          }
        )

        datastore_helper.add_properties(entity,
          {
            "text": unicode(element['text'])
          },
          exclude_from_indexes=True
        )

        return [entity]

class ProcessOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
                '--input',
                dest='input',
                type=str,
                required=False,
                help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')

        parser.add_value_provider_argument(
                '--entity',
                dest='entity',
                type=str,
                required=False,
                help='The entity name')

        parser.add_value_provider_argument(
                '--user',
                dest='user',
                type=str,
                required=False,
                help='The user identifier')

        parser.add_value_provider_argument(
                '--dataset',
                dest='dataset',
                type=str,
                required=False,
                help='The dataset')


def dataflow(argv=None):
    process_options = PipelineOptions().view_as(ProcessOptions)
    p = beam.Pipeline(options=process_options)

    (p
     | 'Read From Text' >> beam.io.ReadFromText(process_options.input, skip_header_lines=0)
     | 'Process CSV' >> beam.ParDo(ProcessCSV(),['text','label'])
     | 'Build entities' >> beam.ParDo(BuildEntities(), process_options.entity, process_options.user, process_options.dataset)
     | 'Write entities into Datastore' >> WriteToDatastore('io-annotator-api'))

    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    dataflow()
