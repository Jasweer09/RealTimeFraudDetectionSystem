import os
import logging

import boto3
import yaml
import mlflow
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler('./fraud_detection_training.log'),
        logging.StreamHandler()
    ]
    )

logger = logging.getLogger(__name__)


class FraudDetectionTraining:
    def __init__(self, config_path='/app/config.yaml'):
        os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = '/usr/bin/git'

        load_dotenv(dotenv_path='/app/.env')
        self.config = self._load_config(config_path)
        # logger.info('Hi starting the init')
        # logger.info(f"AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID')}")
        # logger.info(f"AWS_SECRET_ACCESS_KEY: {os.getenv('AWS_SECRET_ACCESS_KEY')}")
        # logger.info(f"AWS_S3_ENDPOINT_URL: {self.config['mlflow']['s3_endpoint_url']}")
        # logger.info('ending starting the init1')

        os.environ.update({
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            #,
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            #os.getenv('AWS_SECRET_ACCESS_KEY'),
            'AWS_S3_ENDPOINT_URL': self.config['mlflow']['s3_endpoint_url']
        })
        logger.info('ending starting the init')
        self._validate_environment()

        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])


    def _load_config(self, config_path: str) -> dict:
        logger.info('entered the load config')
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info('Configuration loaded successfully')
            return config

        except Exception as e:
            logger.error('Failed to load configuration file: %s', str(e))
            raise e


    def _validate_environment(self):
        required_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD']
        # logger.info('Hi starting the missing')
        # logger.info(f"KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        # logger.info(f"server:{'KAFKA_BOOTSTRAP_SERVERS' in os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        # logger.info(f"KAFKA_USERNAME: {os.getenv('KAFKA_USERNAME')}")
        # logger.info(f"user:{'KAFKA_USERNAME' in os.getenv('KAFKA_USERNAME')}")
        # logger.info(f"KAFKA_PASSWORD: {os.getenv('KAFKA_PASSWORD')}")
        # logger.info(f"password:{'KAFKA_PASSWORD' in os.getenv('KAFKA_PASSWORD')}")
        # logger.info('ending starting the missing')
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        logger.info('Missing variables are {missing_vars}'.format(missing_vars=missing_vars))
        if missing_vars:
            raise ValueError(f'Missing required environment variables: {missing_vars}')
        self._check_minio_connection()
    def _check_minio_connection(self, mlflow_bucket=None):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=self.config['mlflow']['s3_endpoint_url'],
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                )
            bucket = s3.list_buckets()
            bucket_names = [b['Name'] for b in bucket.get('Buckets', [])]
            logger.info('Minio connection verified. Bucket: %s', bucket_names)
            mlflow_bucket = self.config['mlflow'].get('bucket', 'mlflow')
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info('Created missing MLFlow bucket: %s', mlflow_bucket)
        except Exception as e:
            logger.error('Minio connection failed: %s', str(e))

    def trian_model(self):
        pass
