import os
from functools import wraps
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
from contextlib import closing
import datetime
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from flask_table import Table, Col
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json
 

# Configuration
DATABASE=os.environ.get('CASSANDRA_HOST')
cluster = Cluster([DATABASE])
a1_query = 'SELECT store_id, avg(price) as avg_price, avg(raiting) as avg_rating FROM a1_ratings GROUP BY store_id'
a2_query = 'SELECT store_id, sum(value) as total_per_store FROM a2_receipts GROUP BY store_id'


columns_a3 = [
  {
    "field": "store_id", # which is the field's name of data key 
    "title": "Store", # display as the table header's name
    "sortable": True,
  },
  {
    "field": "avg_price",
    "title": "Average price",
    "sortable": True,
  },
  {
    "field": "avg_rating",
    "title": "Average rating",
    "sortable": True,
  }
]

columns_a4 = [
  {
    "field": "store_id", # which is the field's name of data key 
    "title": "Store", # display as the table header's name
    "sortable": True,
  },
  {
    "field": "total_per_store",
    "title": "Total",
    "sortable": True,
  }
]


class receipts_by_store_id(Model):
    __keyspace__="shop_receipts"
    store_id = columns.SmallInt(primary_key=True)
    topic = columns.Text(primary_key=True)
    ts = columns.DateTime()
    value = columns.SmallInt()

class raitings_by_shop_id(Model):
    __keyspace__="shop_raitings"
    store_id = columns.SmallInt(primary_key=True)
    product_name = columns.Text(primary_key=True)
    ts = columns.DateTime()
    price = columns.Float()
    raiting = columns.SmallInt()

class ReceiptsTable(Table):
    store_id = Col('Store')
    topic = Col('Topic')
    ts = Col('TimeStamp')
    value = Col('Value')

class Receipts(object):
    def __init__(self, store_id, topic, ts, value):
        store_id = self.store_id
        topic = self.topic
        ts = self.ts
        value = self.value

class RaitingsTable(Table):
    store_id = Col('Store')
    product_name = Col('Product_Name')
    ts = Col('TimeStamp')
    price = Col('Price')
    raiting = Col('Raiting')

class Raitings(object):
    def __init__(self, store_id, product_name, ts, price, raiting):
        store_id = self.store_id
        product_name = self.product_name
        ts = self.ts
        price = self.price
        raiting = self.raiting

class A1(Table):
    store_id = Col('Store')
    avg_price = Col('Avg price')
    avg_rating = Col('Avg rating')
   
class A2(Table):
    store_id = Col('Store')
    total_per_store = Col('Total per store')



application = Flask(__name__)
application.config.from_object(__name__)


def connect_db(__keyspace__):
    """Connects to the specific database."""
    return connection.setup([DATABASE],__keyspace__, protocol_version=3)

def get_a1_query():
    session = cluster.connect('shop_raitings')
    return session.execute(a1_query)

def get_a2_query():
    session = cluster.connect('shop_receipts')
    return session.execute(a2_query)

def get_ratings():
    connect_db("shop_raitings")
    print raitings_by_shop_id.objects.count()
    return raitings_by_shop_id.objects.all()

def get_receipts():
    connect_db("shop_receipts")
    print receipts_by_store_id.objects.count()
    return receipts_by_store_id.objects.all()

@application.route('/')
def index():
    return render_template('base.html')

@application.route('/ratings')
def ratings():
    entries = get_ratings()
    table = RaitingsTable(entries)
    return table.__html__()

@application.route('/receipts')
def receipts():
    entries = get_receipts()
    table = ReceiptsTable(entries)
    return table.__html__()

@application.route('/dashboard1')
def a1():
    entries = get_a1_query()
    table = A1(entries)
    return table.__html__()

@application.route('/dashboard2')
def a2():
    entries = get_a2_query()
    table = A2(entries)
    return table.__html__()

def get_a3():
    data = []
    session = cluster.connect('shop_raitings')
    session.row_factory = dict_factory
    rows = session.execute(a1_query)
    for row in rows:
        data.append(row)
    return data 
  
@application.route('/analytical_dashboards/ratings')
def a3():
    return render_template("table.html",
      data=get_a3(),
      columns=columns_a3,
      title='Analytical dashboard. Average metrics price and rating by store')

def get_a4():
    data = []
    session = cluster.connect('shop_receipts')
    session.row_factory = dict_factory
    rows = session.execute(a2_query)
    for row in rows:
        data.append(row)
    return data

@application.route('/analytical_dashboards/receipts')
def a4():
    return render_template("table.html",
      data=get_a4(),
      columns=columns_a4,
      title='Analytical dashboard. Total sum by store')
    
if __name__ == "__main__":
    application.run(port=5001)


    



