from src.transform import transform


def test_transform(spark_session):
    df_dict = {
        "fact": spark_session.read.csv(
            "tests/resources/inputs/order_data.csv", header=True, inferSchema=True
        ),
        "lookup": spark_session.read.csv(
            "tests/resources/inputs/zip.csv", header=True, inferSchema=True
        ),
    }

    # call actual function
    df_actual = transform(df_dict)

    # read the extracted data
    expected_df = spark_session.read.csv(
        "tests/resources/expected/*.csv", header=True, inferSchema=True
    )

    assert sorted(expected_df.collect()) == sorted(df_actual.collect()), f"validating transform functionality"