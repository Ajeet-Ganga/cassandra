from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import os
import uuid
import random
import string

# First do this once from cqlsh
#  CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
#  CREATE TABLE users (  user_id int PRIMARY KEY,  fname text,  lname text);

# TO Profile run it like
# python -m profile py.py | grep doit

cluster = Cluster(['10.9.222.224'],protocol_version=1);
session = cluster.connect();
session.set_keyspace('keyspace1')

def doit():
        i = 0
        while i < 10:
                user_id = ''.join([random.choice(string.digits + string.digits) for n in xrange(5)])
                fname = ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(10)])
                lname = ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(10)])
        
                session.execute("INSERT INTO users (user_id,  fname, lname)  VALUES (%s, %s, %s)",(int(user_id),fname,lname));
        
                i = i + 1;

doit();
