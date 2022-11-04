from cgitb import html
import psycopg2
import os, re
import ftplib
from ftplib import FTP
import pandas as pd
import mail
import ftp_sanitization


pg_host = "souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com"
pg_user = os.environ.get("PG_USER")
pg_password = os.environ.get("PG_PASSWORD")
clients_crm_table = "clients_crm"
clients_db_name = "souscritootest"
clients_request = 'SELECT "FirstName", "LastName", "CreationDate","PhoneNumber" FROM public.clients_crm WHERE "CreationDate" = (SELECT max("CreationDate") FROM public.clients_crm);'
create_table_request = """CREATE TABLE public.{} 
                        (client_id VARCHAR(255),
                        FirstName VARCHAR(255),
                        LastName VARCHAR(255),
                        Phone VARCHAR(255),
                        First_Call_Date VARCHAR(255),
                        Call_Hour VARCHAR(255),
                        avg_call_duration VARCHAR(255))
"""
dict = {"apple": "res", "truc": "machin", "orange": "lemon", "lemon": "yelloww"}

dest_pg_host = "127.0.0.1"
dest_pg_user = os.environ.get("DEST_PG_USER")
dest_pg_password = os.environ.get("DEST_PG_PASSWORD")
marketing_table = "clients_crm"
marketing_db_name = "souscritoomarket"


ftp_host = "35.157.119.136"
ftp_password = os.environ.get("FTP_PASSWORD")
ftp_user = os.environ.get("FTP_USER")
ftp_base_path = "/files/raw_calls.csv"
file_copy = "raw_calls.csv"


class Postgresql:
    def __init__(self, host, db_name, user, password):
        try:
            conn = psycopg2.connect(
                host=host, database=db_name, user=user, password=password
            )

            cur = conn.cursor()
            print("cur created")
            # To-Do : make the destination data types clean
            """cur.execute(
                f"SELECT 'CREATE DATABASE {marketing_db_name}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{marketing_db_name}')\gexec; CREATE TABLE {marketing_table} \
                    (client_id VARCHAR(255), FirstName VARCHAR(255), LastName VARCHAR(255), Phone VARCHAR(255), First_Call_Date VARCHAR(255), Call_Hour VARCHAR(255), avg_call_duration VARCHAR(255))")
            """
            cur.execute(create_table_request.format(marketing_table))
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def pg_request(self, host, db_name, user, password, request):
        try:
            conn = psycopg2.connect(
                host=host, database=db_name, user=user, password=password
            )

            cur = conn.cursor()
            cur.execute(request)
            self.last_day_request_result = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def ftp_connect(host, user, password, file_path, destination_file):
    try:
        ftp = FTP(host)
        ftp.login(user, password)
        print("FTP Successfuly connected")
        files = []
        ftp.dir(files.append)
        with open(destination_file, "w") as fp:

            res = ftp.retrlines("RETR " + file_path, fp.write)

            if not res.startswith("226 Transfer complete"):

                print("Download failed")
                if os.path.isfile(destination_file):
                    os.remove(destination_file)

    except ftplib.all_errors as e:
        errorcode_string = str(e).split(None, 1)[0]
        print(errorcode_string)


def clean_names(element):
    return "".join(re.sub("[^A-Za-z0-9\-éè]", "", element))


def get_postgres_source_data():
    """
    Write results in destination database PG marketing
    """
    p = Postgresql(
        host=dest_pg_host,
        db_name=marketing_db_name,
        user=dest_pg_user,
        password=dest_pg_password,
    )
    p.pg_request(pg_host, clients_db_name, pg_user, pg_password, clients_request)
    df = pd.DataFrame(p.last_day_request_result)

    for i in [0, 1]:
        df.loc[:, i] = [clean_names(element) for element in df[i]]
    # df.loc[:] = [clean_names(element) for element in df[0]]
    print(df)

    # Création du body du mail à envoyer
    body = "\n".join(df[0] + " " + df[1])
    body = re.sub("\n", "<br>", body)
    msg = mail.create_email(body)
    print(body)
    df.columns = ["FirstName", "LastName", "CreationDate", "incoming_number1"]
    return df


# Connexion et récupération des données :

# pg_connect(pg_host, clients_db_name, pg_user, pg_password)
ftp_connect(ftp_host, ftp_user, ftp_password, ftp_base_path, file_copy)

# Nettoyage fichier FTP :
df_ftp = ftp_sanitization.clean_ftp_file()

# Get des résultats dans la base de données source
df = get_postgres_source_data()

# Join both data sources
df_out = pd.merge(df, df_ftp, left_on="incoming_number1", right_on="incoming_number1")
df_out.drop("called_number", axis=1, inplace=True)
df_out.rename(columns={"date": "firstCallDate", "incoming_number1":"IncomingNumber", "duration_in_sec": "CallDurationInSec"}, inplace=True)
df_out.to_csv("client.csv", sep=',')
print(df_ftp)
print(df_out)
