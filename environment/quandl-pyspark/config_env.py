#import sparkenv
# -----------------------python basic imports
import quandl
import os
from os.path import expanduser, join, abspath
# import pandas_datareader.data as web
# import Data_Api
# import time
# import sparkenv
# import json

# ---------------------sparks imports
# import sys
from pyspark.shell import sqlContext, sc
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

# ----------------Import pandas and sqlite3
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error

from pyspark.sql import SQLContext
print(sc)
#spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()