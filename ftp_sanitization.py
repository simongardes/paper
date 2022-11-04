import re, itertools
import pandas as pd
import io
from math import isnan

def clean_ftp_file():
    with open("raw_calls.csv", "r+") as file:
        lines = file.read()
        content = str(lines)
        result = re.sub(
            ",",
            lambda m, c=itertools.count(): m.group() if next(c) % 4 else "\n",
            lines,
        )
        result = re.sub("id\n", "id,", result)
        result = re.sub("980801525", ",980801525", result)

    df_ftp = pd.read_csv(io.StringIO(result), sep=",", dtype={"incoming_number1": str})
    df_ftp.reset_index()
    df_ftp.drop("id", axis="columns", inplace=True)
    df_ftp["incoming_number1"] = df_ftp["incoming_number1"].fillna('0')

    for index, row in df_ftp.iterrows():
        if len(row["incoming_number1"]) > 2:
            df_ftp["incoming_number1"][index] = row["incoming_number1"][: -len(str(index + 2))]
            df_ftp["incoming_number1"][index] = "0" + df_ftp["incoming_number1"][index]
        else:
            df_ftp["incoming_number1"][index] = 0
            
    return df_ftp
    #print(df_ftp)
