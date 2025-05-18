def transform(glueContext, dfc) -> DynamicFrame:
    from pyspark.sql.functions import coalesce
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Rename columns to avoid collision
    df = df.withColumnRenamed("TotalCases", "total_cases")
    df = df.withColumnRenamed("NewCases", "new_cases")
    df = df.withColumnRenamed("TotalDeaths", "total_deaths")
    df = df.withColumnRenamed("NewDeaths", "new_deaths")
    df = df.withColumnRenamed("TotalRecovered", "total_recovered")
    df = df.withColumnRenamed("Recovered", "recovered_old")
    df = df.withColumnRenamed("ActiveCases", "active_cases_old")
    df = df.withColumnRenamed("Active", "active_old")
    df = df.withColumnRenamed("Serious,Critical", "serious_critical_old")
    df = df.withColumnRenamed("Critical", "critical_old")
    df = df.withColumnRenamed("ConfirmedLastWeek", "confirmed_last_old")
    df = df.withColumnRenamed("Confirmed", "confirmed_old")

    # Merging logic
    df = df.withColumn("confirmed", coalesce(df["total_cases"], df["new_cases"]))
    df = df.withColumn("deaths", coalesce(df["total_deaths"], df["new_deaths"]))
    df = df.withColumn("recovered", coalesce(df["total_recovered"], df["recovered_old"]))
    df = df.withColumn("active_cases", coalesce(df["active_cases_old"], df["active_old"]))
    df = df.withColumn("serious_critical", coalesce(df["serious_critical_old"], df["critical_old"]))
    df = df.withColumn("confirmed", coalesce(df["confirmed_last_old"], df["confirmed_old"])) 

    # Drop unnecessary intermediate columns
    df = df.drop(
        "total_cases", "new_cases", "total_deaths", "new_deaths",
        "total_recovered", "recovered_old", "active_cases_old", "active_old",
        "serious_critical_old", "critical_old", "confirmed_last_old", "confirmed_old"
    )

    return DynamicFrame.fromDF(df, glueContext, "merged_df")
