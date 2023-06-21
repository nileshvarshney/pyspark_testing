import argparse
import simplejson
from simplejson import errors as simplejson_error


def extract_data_source(spark, sources):
    """
    Loop through each source in the source list and create a dictionary
    of dataset name and its data frame
    :param spark: pyspark session
    :param sources: List of data source dict
    :return: dictionary key -> data set name and value as dataframe
    """
    df_dict = {}
    for source in sources:
        df_dict[source["name"]] = read_source_data(spark, source["storeConfig"])
    return df_dict


def read_source_data(spark, source_config):
    """
    read the provided source file and returns the spark dataframe
    :param spark: spark Session
    :param source_config: input csv file
    :return: pyspark dataframe
    """
    return spark.read.csv(source_config["path"], header=True, inferSchema=True)


def read_job_configuration(job_config_filename):
    """
    Read the provided job configuration file and return a JSON dict
    :param job_config_filename:  job configuration file
    :return: JSON dict
    """
    with open(job_config_filename) as file:
        try:
            return simplejson.load(file)
        except simplejson_error.JSONDecodeError as error:
            raise simplejson_error.JSONDecodeError(
                f"job configuration file error {job_config_filename}.json error: {error}"
            )


def read_command_line_arguments():
    """
    parse command line arguments
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_name", help="specify config file name", action="store")
    parser.add_argument("--env", help="env, dev, pre-prod, prod", action="store")
    return parser.parse_known_args()[0]


def save_to_sink(df, config, env="prod"):
    """
    Given a spark dataframe, env and its sink configurations, the function dumps the data to sink
    :param env: dev, stage, pre-prod, prod
    :param df: spark dataframe
    :param config: sink config
    :return: NA
    """
    sink_conf = config["storeConfig"]
    if config["storeType"] == "object_store":
        df.write.format(sink_conf["format"]).mode(sink_conf["mode"]).save(
            sink_conf["path"].format(env=env)
        )