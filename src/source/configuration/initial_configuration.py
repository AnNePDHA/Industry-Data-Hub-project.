from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

dbutils = get_dbutils(spark)

BLOB_STORAGE_URL = ''
BLOB_CONTAINER = ''
BLOB_CONTAINER_FOLDER_INPUT = ''
BLOB_CONTAINER_FOLDER_PROCESSED = ''
BLOB_CONTAINER_FOLDER_FPR = ''
BLOB_CONTAINER_FOLDER_WRITE_TO_SQL = ''
BLOB_CONTAINER_FOLDER_REFDATA = ''
BLOB_CONTAINER_FOLDER_INPUT_ABFSS = f''
BLOB_CONTAINER_FOLDER_FPR_ABFSS = f''
BLOB_CONTAINER_FOLDER_WRITE_TO_SQL_ABFSS = f''
BLOB_CONTAINER_FOLDER_REFDATA_ABFSS = f''
BLOB_CONTAINER_PROCESS_FOLDER_REFDATA_ABFSS = f''
DELTA_AZURE_OAUTH_TENANT_ID = dbutils.secrets.get(scope = "", key = "")
DELTA_AZURE_OAUTH_CLIENT_ID = dbutils.secrets.get(scope = "", key = "")
DELTA_AZURE_OAUTH_CLIENT_SECRET = dbutils.secrets.get(scope = "", key = "")
DELTA_AZURE_OAUTH_ENDPOINT_TOKEN = f''
DELTA_STORAGE_KEY = dbutils.secrets.get(scope = "", key = "")

SQL_idh_SERVER_URL =  dbutils.secrets.get(scope = '', key = '')
SQL_chia_uat_info_SERVER_URL =  dbutils.secrets.get(scope = '', key = '')
SQL_SERVER_NAME = dbutils.secrets.get(scope = '', key = '')  
SQL_SERVER_URL =  dbutils.secrets.get(scope = '', key = '')
SQL_USERNAME = dbutils.secrets.get(scope = '', key = '')
SQL_PASSWORD = dbutils.secrets.get(scope = '', key = '')
SQL_CHIA_UAT_DATABASE = ''
SQL_CHIA_UAT_INFO_DATABASE = ''

RESOURCE_APP_ID_URL = ""
host_name_in_certificate = ""
FILE_RULE_TSS_URL = ""

CHIA_FOLDER_NAME = ''
CHP_FOLDER_NAME = ''

GOOGLE_API_KEY = ''

DATABRICK_HOSTNAME = ''