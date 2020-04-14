import aws as aws_s3
import snowflake.connector
import calendar
import os
from datetime import datetime, timedelta


def upload(key, table_name):
    ctx = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT')

    sql = """
    copy into ALOOMADB.{} from 's3://twilio-analysis/{}' 
    credentials=(aws_key_id='{}' aws_secret_key='{}')
    file_format = (type = 'CSV' ,skip_header=1 ,field_delimiter = ',', FIELD_OPTIONALLY_ENCLOSED_BY = '"')""".format(
        table_name, key, aws_s3.AWS_ACCESS_KEY_ID,
        aws_s3.AWS_SECRET_ACCESS_KEY)

    cs = ctx.cursor()
    try:
        cs.execute(sql)
    finally:
        cs.close()


date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
aws_s3.year_path_name = date.split('-')[0] + '/'
aws_s3.month_path_name = calendar.month_name[int(date.split('-')[1])] + '/'
path = 'twilio-billing/{}{}{}.csv'.format(aws_s3.year_path_name, aws_s3.month_path_name, date)
table = 'TWILIO.TWILIO_DAILY_COST'
upload(path, table)