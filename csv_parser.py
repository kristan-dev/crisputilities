import pandas as pd
import io
from itertools import islice
import logging

from s3_object_source import S3_Source
from config import cfg
import logger


class CSVParser:  
    @classmethod
    def parse_csv(cls, s3args):
        logging.info("Reading S3 CSV into Dataframe")
        s3_source = S3_Source(s3args=s3args)
        s3_obj = s3_source.S3ObjectDataSource()

        flag = True
        logging.info("Processing Dataframe as chunks")
        for chunk in pd.read_csv(io.BytesIO(s3_obj["Body"].read()),chunksize=1000000,delimiter=",",keep_default_na=False,):
            if flag is True:
                keys = chunk.columns.to_list()
                flag = False
            for row in chunk.iterrows():
                yield cls.form_data(keys=keys, value=row)
    
    @classmethod
    def parse_csv_from_file(cls, s3args, file_name):
        logging.info("Downloading file from s3 bucket")
        source_s3 = S3_Source(s3args=s3args)
        source_s3.download_file_as_temp(file_abspath=file_name)

        flag = True
        logging.info("Processing Dataframe as chunks")
        for chunk in pd.read_csv(file_name,chunksize=1000000,delimiter=",",keep_default_na=False,):
            if flag is True:
                keys = chunk.columns.to_list()
                flag = False
            for row in chunk.iterrows():
                yield cls.form_data(keys=keys, value=row)

    @staticmethod
    def form_data(keys, value):
        value = value[1]
        data = {}
        for key in keys:
            data[key] = value[key]
        return data


if __name__ == "__main__":
    # csv_rows = CSVParser.parse_csv()
    # batch_size = 10000
    # while True:
    #   rows =list(islice(csv_rows, 0, batch_size))
    #   if len(rows) <= 0:
    #       break
    pass
