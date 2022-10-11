#import psycopg2
import os
import ftplib
from ftplib import FTP

pg_host = "souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com"
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
clients_crm_table = 'clients_crm'
clients_db_name = 'souscritootest'
clients_request = 'SELECT FirstName, LastName FROM public.clients_crm WHERE CreationDate="2016-01-17";'


dest_pg_host = "localhost"
dest_pg_user = os.environ.get('DEST_PG_USER')
dest_pg_password = os.environ.get('DEST_PG_PASSWORD')
marketing_table = 'clients_crm'
marketing_db_name = 'souscritoomarket'



ftp_host = '35.157.119.136'
ftp_password = os.environ.get('FTP_PASSWORD')
ftp_user = os.environ.get('FTP_USER')
ftp_base_path = '/files/raw_calls.csv'
file_copy = 'raw_calls.csv'


class Postgresql:
    def __init__(self, host, db_name, user, password):
        try:
            conn = psycopg2.connect(
                host = host,
                database = db_name,
                user = user,
                password = password)

            cur = conn.cursor()

            #To-Do : make the destination data types clean
            cur.execute(
                f"SELECT 'CREATE DATABASE {marketing_db_name}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{marketing_db_name}')\gexec; CREATE TABLE {marketing_table} \
                    (client_id VARCHAR(255), FirstName VARCHAR(255), LastName VARCHAR(255), Phone VARCHAR(255), First_Call_Date VARCHAR(255), Call_Hour VARCHAR(255), avg_call_duration VARCHAR(255))")

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)



    def pg_request(host, db_name, user, password, request):
        try:
            conn = psycopg2.connect(
                host = host,
                database = db_name,
                user = user,
                password = password)

            cur = conn.cursor()
            cur.execute(request)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    




def ftp_connect(host, user, password, file_path, destination_file):
    try:
        ftp = FTP(host)
        ftp.login(user, password)
        print("FTP Successfuly connected")
        files = []
        ftp.dir(files.append)
        with open(destination_file, 'w') as fp:
            
            res = ftp.retrlines('RETR ' + file_path, fp.write)
            
            if not res.startswith('226 Transfer complete'):
                
                print('Download failed')
                if os.path.isfile(destination_file):
                    os.remove(destination_file)



    except ftplib.all_errors as e:
        errorcode_string = str(e).split(None, 1)[0]




def write_results():
    """
    Write results in destination database PG marketing
    """
    p = Postgresql(host = dest_pg_host, db_name = marketing_db_name, user = dest_pg_user, password = dest_pg_password)
    p.pg_request(pg_host, clients_db_name, pg_user, pg_password, clients_request)




#Connexion et récupération des données :

#pg_connect(pg_host, clients_db_name, pg_user, pg_password)
ftp_connect(ftp_host, ftp_user, ftp_password, ftp_base_path, file_copy)


""" Cleaning


#Côté FTP :RegEx sur le % et autres caractères non alpha numériques etc...

#Côté PostgreSQL : RegEx sur les ":" et autres caractères 

->>> df.strings.str.replace('[^a-zA-Z]','')


"""


#Ecriture des résultats dans la base de données cible (marketing)
write_results()