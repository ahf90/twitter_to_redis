from datetime import datetime
import logging.config
from pythonjsonlogger import jsonlogger
from prometheus_client import start_http_server
from collect_tweets import initiate_collection


class ElkJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(ElkJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['@timestamp'] = datetime.utcnow().isoformat()
        log_record['level'] = record.levelname
        log_record['logger'] = record.name


logging.config.fileConfig('logging.conf')
logger = logging.getLogger('equityvine_logger')

if __name__ == '__main__':
    # Start your prometheus metrics exporter
    start_http_server(8000)
    initiate_collection()
