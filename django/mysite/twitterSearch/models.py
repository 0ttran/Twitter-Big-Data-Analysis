from __future__ import unicode_literals

from django.db import models

# Create your models here.
import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class popularhashtags(Model):
    longitude    = columns.Integer(primary_key=True)
    latitude  = columns.Integer(primary_key=True)
    occurence    = columns.Integer(primary_key=True)
    hashtag   = columns.Text(primary_key=True)