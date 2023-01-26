import sys
import json
import logging

import pandas as pd
import requests
from google.cloud import bigquery, storage, secretmanager


# constant ============================================
bq_client = bigquery.Client()
gcs_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

SLACK_WEBHOOK_URL = secret_client.access_secret_version(request={'name': 'projects/831232013080/secrets/SECRETARY_BOT_V2_SLACK_WEBHOOK_GCP_NOTICE_URL/versions/latest'}).payload.data.decode('UTF-8')


# logger ===============================================
class JsonFormatter(logging.Formatter):
    def format(self, log):
        return json.dumps({
            'level': log.levelname,
            'message': log.getMessage(),
        })


formatter = JsonFormatter()
stream = logging.StreamHandler(stream=sys.stdout)
stream.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(stream)


# functions ==========================================
def bqloader(event, context):
    logger.info('===== START assets bqloader =====')
    logger.info('Event ID={}'.format(context.event_id))
    logger.info('Event type={}'.format(context.event_type))
    logger.info('Bucket={}'.format(event['bucket']))
    logger.info('File={}'.format(event['name']))
    logger.info('Metageneration={}'.format(event['metageneration']))
    logger.info('Created={}'.format(event['timeCreated']))
    logger.info('Updated={}'.format(event['updated']))

    try:
        # GCSに配置されたファイルをダウンロード
        logger.info('----- download gcs asset file -----')
        bucket = gcs_client.get_bucket(event['bucket'])
        blob = bucket.blob(event['name'])
        blob.download_to_filename('/tmp/' + event['name'])

        # 列名を変更
        logger.info('----- rename row name -----')
        df = pd.read_csv('/tmp/' + event['name'], header=0, encoding='shift-jis')
        df.rename(columns={'日付': 'date', '合計（円）': 'total', '預金・現金・暗号資産（円）': 'cash', '投資信託（円）': 'investment_trust', 'ポイント（円）': 'point'}, inplace=True)

        # 日付をyyyy-mm-ddに変更
        df['date'] = df['date'].str.replace('/', '-')

        # NaNを含む行を削除する
        df = df.dropna(how='any')
        logger.debug('dataframe={}'.format(df))

        # BQにインサート
        logger.info('----- insert bq -----')
        table = bq_client.get_table('slackbot-288310.my_dataset.asset_trend')
        result = bq_client.insert_rows_from_dataframe(table, df)

        logger.info('----- post slack chat message -----')
        data = {
            "attachments": [
                {
                    "color": "ffffff",
                    "text": '資産推移をBQに登録しました。',
                }
            ]
        }
        logger.info('payload={}'.format(data))

        json_data = json.dumps(data).encode("utf-8")
        response = requests.post(SLACK_WEBHOOK_URL, json_data)
        logger.info('response={}'.format(response))
        logger.info('response text={}'.format(response.text))

    except Exception as e:
        logger.error(e)
    finally:
        logger.info('===== END assets bqloader =====')
