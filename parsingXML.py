from lxml import etree
import requests
from datetime import timedelta, date
from collections import namedtuple
import psycopg2
import time

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def generateObj(CharCode, Value, Date):
    date = Date.strftime("%Y-%m-%d")
    Id = CharCode + date
    c = Currency(date, Value, CharCode, Id)
    return c


conn = psycopg2.connect(dbname='database', user='user',
                        password='password', host='host')


start_date = date(2019, 4, 6)
end_date = date(2019, 9, 30)


class Currency(object):
    def __init__(self, Date, Value, CharCode, Id):
        self.Date = Date
        self.Value = Value
        self.CharCode = CharCode
        self.Id = Id


def parseXML(date):
    url = "http://www.cbr.ru/scripts/XML_daily.asp"
    querystring = {"date_req":date.strftime("%d.%m.%Y")}
    headers = {
        'Authorization': "Bearer 56656",
        'User-Agent': "PostmanRuntime/7.15.2",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "89ecb46c-bb25-480a-913a-5283843e2e56,060a620e-ba9e-4e1a-93be-4e7efbf5f7cc",
        'Host': "www.cbr.ru",
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)

    root = etree.fromstring(response.content)

    for appt in root.getchildren():
        for elem in appt.getchildren():
            if elem.tag == 'CharCode':
                CharCode = elem.text
            elif elem.tag == 'Value':
                Value = float(elem.text.replace(",", "."))
        c = generateObj(CharCode, Value, date)
        insertToBD(c, conn)

def insertToBD(c, conn):
    try:
        cursor = conn.cursor()
        postgres_insert_query = """ INSERT INTO currency_rate ("Date","Value","CharCode","Id") VALUES (%s, %s, %s, %s) """
        record_to_insert = (c.Date, c.Value, c.CharCode, c.Id)
        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        print(record_to_insert, 'Suc')
    except Exception as e:
        conn.rollback()
        print(e, 'UnSuc')


for single_date in daterange(start_date, end_date):
    parseXML(single_date)
