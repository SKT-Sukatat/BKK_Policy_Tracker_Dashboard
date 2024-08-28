def traffy_transform(TRAFFY_RECORDS_API):
    # Start measuring for Spark
    start_time = time.time()
    start_memory = get_memory_usage()

    # Read the CSV data from the REST API to Pandas DataFrame
    df_traffy_all = pd.read_csv(TRAFFY_RECORDS_API)

    # Convert the Pandas DataFrame to PySpark DataFrame
    df_traffy_all = spark.read.csv(df_traffy_all, header=True, inferSchema=True)

    # Select the required columns
    df_traffy_all = df_traffy_all.select('ticket_id', 'timestamp', 'type', 'organization', 'comment', 'photo',
                                        'photo_after', 'coords', 'address', 'subdistrict', 'district',
                                        'province', 'state', 'star', 'count_reopen', 'last_activity')

    # Convert the 'timestamp' column to datetime objects
    df_traffy_all = df_traffy_all.withColumn('timestamp',
                                            to_timestamp(col('timestamp')).cast('timestamp'))

    # Remove microseconds and timezone information
    df_traffy_all = df_traffy_all.withColumn('timestamp',
                                            date_format(col('timestamp'), 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))

    # Convert the 'last_activity' column to datetime objects
    df_traffy_all = df_traffy_all.withColumn('last_activity',
                                            to_timestamp(col('last_activity')).cast('timestamp'))

    # Remove microseconds and timezone information
    df_traffy_all = df_traffy_all.withColumn('last_activity',
                                            date_format(col('last_activity'), 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))

    # Set datetime threshold for data
    date_threshold = '2022-04-01 00:00:00'
    df_traffy_all = df_traffy_all.filter(col('timestamp') > date_threshold)

    # Replace '{' and '}' in the 'type' column
    df_traffy_all = df_traffy_all.withColumn('type',
                                            regexp_replace(regexp_replace(col('type'), '{', ''), '}', ''))

    # Drop rows where 'ticket_id' or 'type' is NaN
    df_traffy_all = df_traffy_all.dropna(subset=['ticket_id', 'type'])

    # Replace ' ' with 'อื่นๆ' in the 'type' column
    df_traffy_all = df_traffy_all.withColumn('type',
                                            when(col('type') == '', 'อื่นๆ').otherwise(col('type')))

    print("Data Transformed Successfully, Continue Loading to GCS")\

    end_time = time.time()
    end_memory = get_memory_usage()
    print(f"PySpark - Time: {end_time - start_time} seconds")
    print(f"PySpark - Memory: {end_memory - start_memory} MB")