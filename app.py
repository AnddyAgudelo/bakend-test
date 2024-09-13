import os
import pandas as pd
import json

from pandas import DataFrame

from utils.error_handler import error_handler
from utils.logger import logger


@error_handler
def read_spec(spec_file: str) -> DataFrame:
    logger.info(f"Reading spec file: {spec_file}")
    df_spec = pd.read_csv(spec_file)
    logger.info(f"df_spec: {df_spec}")
    return df_spec


@error_handler
def process_data_file(input_file: str, df_spec: DataFrame) -> DataFrame:
    logger.info(f"Processing file: {input_file}")
    widths = df_spec["width"].tolist()

    df_data = pd.read_fwf(input_file, widths=widths, names=df_spec["column name"])

    logger.info(f"Applying data types.")
    for _, row in df_spec.iterrows():
        col_name = row["column name"]
        data_type = row["datatype"]

        if data_type == "INTEGER":
            df_data[col_name] = df_data[col_name].astype(int)
        elif data_type == "BOOLEAN":
            df_data[col_name] = df_data[col_name].astype(bool)

    logger.info(f"df_data: {df_data}")
    return df_data


@error_handler
def write_ndjson(df_data, output_file):
    logger.info(f"Writing NDJSON file: {output_file}")
    with open(output_file, "w") as ndjson_file:
        for _, row in df_data.iterrows():
            json.dump(row.to_dict(), ndjson_file)
            ndjson_file.write("\n")


def main():
    logger.info("Starting file processing")
    specs_dir = "specs"
    data_dir = "data"
    output_dir = "output"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")

    for data_file in os.listdir(data_dir):
        if data_file.endswith(".txt"):
            logger.info(f"Processing file: {data_file}")
            spec_file_name = data_file.split("_")[0]
            spec_file = os.path.join(specs_dir, f"{spec_file_name}.csv")

            df_spec = read_spec(spec_file)
            if df_spec is None:
                logger.error(f"Could not read spec for {data_file}")
                continue

            input_file = os.path.join(data_dir, data_file)
            output_file = os.path.join(output_dir, f"{os.path.splitext(data_file)[0]}.ndjson")

            df_data = process_data_file(input_file, df_spec)

            if df_data is not None:
                write_ndjson(df_data, output_file)
                logger.info(f"File processed: {data_file} - {os.path.basename(output_file)}")
            else:
                logger.error(f"Could not process file: {data_file}")

            logger.info("File processing completed.")


if __name__ == "__main__":
    main()
