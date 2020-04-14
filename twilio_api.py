import calendar
import os
import csv
import aws as aws_s3
import io
from collections import OrderedDict
from twilio.rest import Client
from datetime import datetime, timedelta


first_acc = {'account_sid': os.getenv('FIRST_ACCOUNT_SID'), 'auth_token': os.getenv('FIRST_AUTH_TOKEN')}
second_acc = {'account_sid': os.getenv('SECOND_ACCOUNT_SID'), 'auth_token': os.getenv('SECOND_AUTH_TOKEN')}

account_list = [first_acc, second_acc]

categories = ['sms', 'sms-messages-carrierfees', 'mms', 'calls', 'calls-client', 'phonenumbers',
              'authy-phone-verifications', 'authy-sms-outbound', 'authy-calls-outbound']
other_categories = ['monitor-writes', 'monitor-reads', 'monitor-storage', 'studio-engagements', 'premiumsupport',
                    'lookups', 'wireless-usage', 'calls-text-to-speech']

row = OrderedDict()
row['Date'] = ''
row['Account'] = ''
row['SID'] = ''
row['Category'] = ''
row['Quantity'] = 0
row['Price'] = 0.0


def get_category_count(account, category, day):
    records = account.usage.records.daily.list(category=category, end_date=day, start_date=day,
                                               include_subaccounts=False)
    return int(float(records[0].usage))


def get_category_price(account, category, day):
    records = account.usage.records.daily.list(category=category, end_date=day, start_date=day,
                                               include_subaccounts=False)
    return round(float(records[0].price), 4)


def get_subaccounts_list(account):
    acc_list = account.api.accounts.list()
    return acc_list


def daily(day):
    table = io.StringIO()
    writer = csv.writer(table)
    writer.writerow(list(row.keys()))
    row['Date'] = day
    for account in account_list:
        acc = Client(account['account_sid'], account['auth_token'])
        for sub_acc in get_subaccounts_list(acc):
            count = 0
            price = 0.0
            row['Account'] = sub_acc.friendly_name
            row['SID'] = sub_acc.sid
            for category in categories:
                row['Category'] = category
                if category == 'sms':
                    try:
                        row['Quantity'] = round(get_category_count(sub_acc, category, day)
                                                - get_category_count(sub_acc, 'authy-sms-outbound', day), 4)
                        row['Price'] = round(get_category_price(sub_acc, category, day)
                                             - get_category_price(sub_acc, 'authy-sms-outbound', day), 4)
                    except Exception:
                        row['Quantity'] = 0
                        row['Price'] = 0.0
                elif category == 'calls':
                    try:
                        row['Quantity'] = round(get_category_count(sub_acc, category, day)
                                                - get_category_count(sub_acc, 'authy-calls-outbound', day), 4)
                        row['Price'] = round(get_category_price(sub_acc, category, day)
                                             - get_category_price(sub_acc, 'authy-calls-outbound', day), 4)
                    except Exception:
                        row['Quantity'] = 0
                        row['Price'] = 0.0
                else:
                    try:
                        row['Quantity'] = get_category_count(sub_acc, category, day)
                        row['Price'] = get_category_price(sub_acc, category, day)
                    except Exception:
                        row['Quantity'] = 0
                        row['Price'] = 0.0
                writer.writerow(list(row.values()))
            for category in other_categories:
                try:
                    count = count + get_category_count(sub_acc, category, day)
                    price = price + get_category_price(sub_acc, category, day)
                except Exception:
                    continue
            row['Category'] = 'other'
            row['Quantity'] = count
            row['Price'] = price
            writer.writerow(list(row.values()))
    aws_s3.upload_file(table, aws_s3.year_path_name + aws_s3.month_path_name + day + '.csv')


date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
aws_s3.year_path_name = date.split('-')[0] + '/'
aws_s3.month_path_name = calendar.month_name[int(date.split('-')[1])] + '/'
daily(date)
