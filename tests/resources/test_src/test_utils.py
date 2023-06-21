from src.utils import extract_data_source
from unittest.mock import patch


def test_extract_data_source(spark_session):
    sources = [
        {
            "name": "fact",
            "storeType": "GCS",
            "storeConfig": {
                "path": "tests/resources/input/order_data.csv",
                "format": "csv",
            },
        },
        {
            "name": "lookup",
            "storeType": "GCS",
            "storeConfig": {
                "path": "tests/resources/input/zip.csv",
                "format": "csv",
            },
        },
    ]
    mock_df = spark_session.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])

    # Frame the expected data
    expected_dict = {"fact": mock_df, "lookup": mock_df}

    with patch("src.utils.read_source_data", spec=True) as mock_extract_data_source:
        mock_extract_data_source.return_value = mock_df
        actual_dict = extract_data_source(spark_session, sources)

    # check the data frame returned from function is same as expected
    assert sorted(expected_dict["fact"].collect()) == sorted(actual_dict["fact"].collect()) and \
           sorted(expected_dict["lookup"].collect()) == sorted(actual_dict["lookup"].collect())





    assert 1 == 1
