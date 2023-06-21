from pyspark.sql import SparkSession
from src.transform import transform
from src.utils import (
    extract_data_source,
    read_job_configuration,
    read_command_line_arguments,
    save_to_sink
)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("pyspark").getOrCreate()
    args = read_command_line_arguments()
    config = read_job_configuration(args.job_config_filename)

    # extract
    df_dict = extract_data_source(spark, config["source"])

    # transform
    df_transformed = transform(df_dict)

    # Load
    save_to_sink(df_transformed, config["sink"], args.env)