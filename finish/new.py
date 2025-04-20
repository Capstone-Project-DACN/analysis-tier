def prepare_dataframe(batch_df, data_type):
    """Prepare and enhance the dataframe with time components
    
    Parameters:
    -----------
    batch_df : DataFrame
        The batch DataFrame to process
    data_type : str
        Type of data being processed (household or ward)
        
    Returns:
    --------
    DataFrame : The processed dataframe with additional time columns
    """
    from pyspark.sql.functions import col, split, hour, minute
    
    # Add hour and minute columns from the formatted_timestamp
    enhanced_df = batch_df.withColumn("hour_val", hour(col("formatted_timestamp"))) \
                         .withColumn("minute_val", minute(col("formatted_timestamp"))) \
                         .withColumn("date_part", split(col("formatted_timestamp"), " ").getItem(0))
    
    # For household data
    if data_type == 'household':
        return enhanced_df.select(
            "device_id", 
            "city_id",
            "district_id",
            "electricity_usage_kwh", 
            "voltage", 
            "current", 
            "formatted_timestamp",
            "date_part",
            "hour_val",
            "minute_val"
        )
    # For ward data
    else:  # ward
        return enhanced_df.select(
            "device_id",
            "city_id",
            "district_id",
            "total_electricity_usage_kwh", 
            "formatted_timestamp",
            "date_part",
            "hour_val",
            "minute_val"
        )
        
def update_statistics(batch_count, data_type, stats):
    """Update monitoring statistics
    
    Parameters:
    -----------
    batch_count : int
        Number of records in the batch
    data_type : str
        Type of data being processed (household or ward)
    stats : dict
        Statistics dictionary to update
    """
    stats['last_10_batch_sizes'].append(batch_count)
    
    if data_type == 'household':
        stats['household_batches_processed'] += 1
        stats['household_records_processed'] += batch_count
    else:  # ward
        stats['ward_batches_processed'] += 1
        stats['ward_records_processed'] += batch_count
        
    stats['total_records_processed'] += batch_count

def write_minute_data(minute_record, data_type, device_id, city_id, district_id, date_val, hour_val, minute_val, s3_bucket):
    """Write data at the minute level
    
    Parameters:
    -----------
    minute_record : DataFrame
        DataFrame containing the latest record for this minute
    data_type : str
        Type of data being processed (household or ward)
    device_id, city_id, district_id : str
        Identifiers for the record
    date_val, hour_val, minute_val : str/int
        Time components
    s3_bucket : str
        The S3 bucket name to write to
        
    Returns:
    --------
    tuple : (success_count, error_count)
    """
    minute_path = f"s3a://{s3_bucket}/{data_type}/{device_id}/{city_id}/{district_id}/{date_val}/{hour_val}/{minute_val}.json"
    
    try:
        # Write in overwrite mode to ensure only the latest data is kept
        minute_record.write \
            .mode("overwrite") \
            .json(minute_path)
        
        return 1, 0
        
    except Exception as e:
        error_msg = f"Error writing minute-level {data_type} records to {minute_path}: {e}"
        log_message(error_msg, "ERROR")
        stats['errors'][f'{data_type}_write_error'] += 1
        return 0, 1

def write_hour_data(hour_record, data_type, device_id, city_id, district_id, date_val, hour_val, s3_bucket):
    """Write data at the hour level
    
    Parameters:
    -----------
    hour_record : DataFrame
        DataFrame containing the latest record for this hour
    data_type : str
        Type of data being processed (household or ward)
    device_id, city_id, district_id : str
        Identifiers for the record
    date_val, hour_val : str/int
        Time components
    s3_bucket : str
        The S3 bucket name to write to
        
    Returns:
    --------
    tuple : (success_count, error_count)
    """
    hour_path = f"s3a://{s3_bucket}/{data_type}/{device_id}/{city_id}/{district_id}/{date_val}/{hour_val}.json"
    
    try:
        # Write in overwrite mode to ensure only the latest data is kept
        hour_record.write \
            .mode("overwrite") \
            .json(hour_path)
        
        return 1, 0
        
    except Exception as e:
        error_msg = f"Error writing hour-level {data_type} records to {hour_path}: {e}"
        log_message(error_msg, "ERROR")
        stats['errors'][f'{data_type}_write_error'] += 1
        return 0, 1

def write_day_data(day_record, data_type, device_id, city_id, district_id, date_val, s3_bucket):
    """Write data at the day level
    
    Parameters:
    -----------
    day_record : DataFrame
        DataFrame containing the latest record for this day
    data_type : str
        Type of data being processed (household or ward)
    device_id, city_id, district_id : str
        Identifiers for the record
    date_val : str
        Date value
    s3_bucket : str
        The S3 bucket name to write to
        
    Returns:
    --------
    tuple : (success_count, error_count)
    """
    day_path = f"s3a://{s3_bucket}/{data_type}/{device_id}/{city_id}/{district_id}/{date_val}.json"
    
    try:
        # Write in overwrite mode to ensure only the latest data is kept
        day_record.write \
            .mode("overwrite") \
            .json(day_path)
        
        return 1, 0
        
    except Exception as e:
        error_msg = f"Error writing day-level {data_type} records to {day_path}: {e}"
        log_message(error_msg, "ERROR")
        stats['errors'][f'{data_type}_write_error'] += 1
        return 0, 1

def get_latest_records(records, spark, time_col="formatted_timestamp"):
    """Extract the latest records from a DataFrame, sorted by timestamp
    
    Parameters:
    -----------
    records : list of DataFrame
        List of DataFrames to union and sort
    spark : SparkSession
        The Spark session
    time_col : str
        Name of the timestamp column to sort by
        
    Returns:
    --------
    DataFrame : The latest record
    """
    from pyspark.sql.functions import col
    
    if not records:
        return None
        
    # Union all records
    union_df = spark.createDataFrame([], records[0].schema)
    for record in records:
        union_df = union_df.union(record)
    
    # Sort to get the latest record
    return union_df.orderBy(col(time_col).desc()).limit(1)

def process_minute_records(hour_records, data_type, device_id, city_id, district_id, date_val, hour_val, s3_bucket, spark):
    """Process and write minute-level records
    
    Parameters:
    -----------
    hour_records : DataFrame
        Records for a specific hour
    data_type, device_id, city_id, district_id, date_val, hour_val : various
        Identifiers and time components
    s3_bucket : str
        The S3 bucket name
    spark : SparkSession
        The Spark session
        
    Returns:
    --------
    tuple : (success_count, error_count, latest_records_list)
    """
    from pyspark.sql.functions import col, lit
    
    success_count, error_count = 0, 0
    latest_minute_records = []
    
    # Get all unique minutes
    minute_groups = hour_records.select("minute_val").distinct()
    
    for minute_row in minute_groups.collect():
        minute_val = minute_row["minute_val"]
        
        # Filter for this minute
        minute_records = hour_records.filter(col("minute_val") == lit(minute_val))
        
        # Sort by timestamp to get the latest record for this minute
        latest_minute_record = minute_records.orderBy(col("formatted_timestamp").desc()).limit(1)
        
        # Add to the list for hour-level aggregation
        latest_minute_records.append(latest_minute_record)
        
        # Write minute-level data
        s_count, e_count = write_minute_data(
            latest_minute_record, data_type, device_id, city_id, district_id, 
            date_val, hour_val, minute_val, s3_bucket
        )
        success_count += s_count
        error_count += e_count
    
    return success_count, error_count, latest_minute_records

def process_hour_records(date_records, data_type, device_id, city_id, district_id, date_val, s3_bucket, spark):
    """Process and write hour-level records
    
    Parameters:
    -----------
    date_records : DataFrame
        Records for a specific date
    data_type, device_id, city_id, district_id, date_val : various
        Identifiers and time components
    s3_bucket : str
        The S3 bucket name
    spark : SparkSession
        The Spark session
        
    Returns:
    --------
    tuple : (success_count, error_count, latest_records_list)
    """
    from pyspark.sql.functions import col, lit
    
    success_count, error_count = 0, 0
    latest_hour_records = []
    
    # Get all unique hours
    hour_groups = date_records.select("hour_val").distinct()
    
    for hour_row in hour_groups.collect():
        hour_val = hour_row["hour_val"]
        
        # Filter for this hour
        hour_records = date_records.filter(col("hour_val") == lit(hour_val))
        
        # Process minutes for this hour
        s_count, e_count, latest_minute_records = process_minute_records(
            hour_records, data_type, device_id, city_id, district_id, 
            date_val, hour_val, s3_bucket, spark
        )
        success_count += s_count
        error_count += e_count
        
        # Get the latest record from all minutes in this hour
        latest_hour_record = get_latest_records(latest_minute_records, spark)
        
        if latest_hour_record:
            # Add to the list for day-level aggregation
            latest_hour_records.append(latest_hour_record)
            
            # Write hour-level data
            s_count, e_count = write_hour_data(
                latest_hour_record, data_type, device_id, city_id, district_id, 
                date_val, hour_val, s3_bucket
            )
            success_count += s_count
            error_count += e_count
    
    return success_count, error_count, latest_hour_records

def process_date_records(device_records, data_type, device_id, city_id, district_id, s3_bucket, spark):
    """Process and write date-level records
    
    Parameters:
    -----------
    device_records : DataFrame
        Records for a specific device
    data_type, device_id, city_id, district_id : various
        Identifiers
    s3_bucket : str
        The S3 bucket name
    spark : SparkSession
        The Spark session
        
    Returns:
    --------
    tuple : (success_count, error_count)
    """
    from pyspark.sql.functions import col, lit
    
    success_count, error_count = 0, 0
    
    # Get all unique dates
    date_groups = device_records.select("date_part").distinct()
    
    for date_row in date_groups.collect():
        date_val = date_row["date_part"]
        
        # Filter for this date
        date_records = device_records.filter(col("date_part") == lit(date_val))
        
        # Process hours for this date
        s_count, e_count, latest_hour_records = process_hour_records(
            date_records, data_type, device_id, city_id, district_id, 
            date_val, s3_bucket, spark
        )
        success_count += s_count
        error_count += e_count
        
        # Get the latest record from all hours in this day
        latest_day_record = get_latest_records(latest_hour_records, spark)
        
        if latest_day_record:
            # Write day-level data
            s_count, e_count = write_day_data(
                latest_day_record, data_type, device_id, city_id, district_id, 
                date_val, s3_bucket
            )
            success_count += s_count
            error_count += e_count
    
    return success_count, error_count

def process_devices(processed_df, data_type, s3_bucket, spark):
    """Process all devices in the DataFrame
    
    Parameters:
    -----------
    processed_df : DataFrame
        The processed DataFrame
    data_type : str
        Type of data being processed
    s3_bucket : str
        The S3 bucket name
    spark : SparkSession
        The Spark session
        
    Returns:
    --------
    tuple : (success_count, error_count)
    """
    from pyspark.sql.functions import col, lit
    
    success_count, error_count = 0, 0
    
    # Group data by device_id, city_id, district_id
    device_groups = processed_df.select("device_id", "city_id", "district_id").distinct()
    
    for row in device_groups.collect():
        device_id = row["device_id"]
        city_id = row["city_id"]
        district_id = row["district_id"]
        
        # Filter records for this device, city, district
        device_records = processed_df.filter(
            (col("device_id") == lit(device_id)) & 
            (col("city_id") == lit(city_id)) & 
            (col("district_id") == lit(district_id))
        )
        
        # Process dates for this device
        s_count, e_count = process_date_records(
            device_records, data_type, device_id, city_id, district_id, 
            s3_bucket, spark
        )
        success_count += s_count
        error_count += e_count
    
    return success_count, error_count

def log_performance_metrics(batch_count, data_type, batch_id, elapsed, success_count, error_count):
    """Log performance metrics
    
    Parameters:
    -----------
    batch_count : int
        Number of records in the batch
    data_type, batch_id : str
        Identifiers
    elapsed : float
        Elapsed time in seconds
    success_count, error_count : int
        Number of successful and failed operations
    """
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Success: {success_count}, Errors: {error_count}")

def update_timing_stats(elapsed, data_type, stats):
    """Update timing statistics
    
    Parameters:
    -----------
    elapsed : float
        Elapsed time in seconds
    data_type : str
        Type of data being processed
    stats : dict
        Statistics dictionary to update
    """
    batch_time_ms = elapsed * 1000
    
    if data_type == 'household':
        stats['household_processing_time_ms'] += batch_time_ms
    else:  # ward
        stats['ward_processing_time_ms'] += batch_time_ms
        
    stats['last_10_processing_times'].append(batch_time_ms)

def write_data_to_minio(batch_df, batch_id, data_type, spark, s3_bucket):
    """Performance-optimized function to write data to MinIO using hierarchical JSON format
    
    Parameters:
    -----------
    batch_df : DataFrame
        The batch DataFrame to process
    batch_id : str
        Identifier for this batch
    data_type : str
        Type of data being processed (household or ward)
    spark : SparkSession
        The Spark session
    s3_bucket : str
        The S3 bucket name to write to
    """
    import time
    
    batch_start_time = time.time()
    batch_count = batch_df.count()
    
    # Update monitoring stats
    update_statistics(batch_count, data_type, stats)
    
    if batch_count == 0:
        log_message(f"{data_type.capitalize()} Batch {batch_id}: No data to process")
        return
        
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Processing {batch_count} records")
    
    # Prepare and enhance the dataframe
    processed_df = prepare_dataframe(batch_df, data_type)
    
    # Process all devices and write data
    success_count, error_count = process_devices(processed_df, data_type, s3_bucket, spark)
    
    # Track performance metrics
    batch_end_time = time.time()
    elapsed = batch_end_time - batch_start_time
    
    # Update timing stats
    update_timing_stats(elapsed, data_type, stats)
    
    # Log performance metrics
    log_performance_metrics(batch_count, data_type, batch_id, elapsed, success_count, error_count)