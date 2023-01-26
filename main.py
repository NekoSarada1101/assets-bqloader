import json

import pandas as pd
import requests
from google.cloud import bigquery, storage, secretmanager


gsc_service_account_info = json.load(open('cloud_storage_credentials.json'))
gcs_credentials = service_account.Credentials.from_service_account_info(gsc_service_account_info)
gcs_client = storage.Client(
    credentials=gcs_credentials,
    project=gcs_credentials.project_id,
)

SLACK_WEBHOOK_URL = secret_client.access_secret_version(request={'name': 'projects/831232013080/secrets/SECRETARY_BOT_V2_SLACK_WEBHOOK_GCP_NOTICE_URL/versions/latest'}).payload.data.decode('UTF-8')



def bqloader(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    # GCSに配置されたファイルをダウンロード
    bucket = gcs_client.get_bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    blob.download_to_filename('/tmp/' + event['name'])

    # 列名を変更
    df = pd.read_csv('/tmp/' + event['name'], header=0, encoding='shift-jis')
    df.rename(columns={'日付': 'date', '合計（円）': 'total', '預金・現金・暗号資産（円）': 'cash', '投資信託（円）': 'investment_trust', 'ポイント（円）': 'point'}, inplace=True)

    # 日付をyyyy-mm-ddに変更
    df['date'] = df['date'].str.replace('/', '-')

    # NaNを含む行を削除する
    df = df.dropna(how='any')
    print(df)

    # BQにインサート
    table = bq_client.get_table('slackbot-288310.my_dataset.asset_trend')
    result = bq_client.insert_rows_from_dataframe(table, df)
    print(result)

    data = {
        "attachments": [
            {
                "color": "ffffff",
                "text": '資産推移をBQに登録しました。',
            }
        ]
    }
    print(data)

    json_data = json.dumps(data).encode("utf-8")
    response = requests.post(SLACK_INCOMING_WEBHOOK_URL, json_data)
    print(response)
    print(response.text)
