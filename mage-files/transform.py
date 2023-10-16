import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(uberDF, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
        # Specify your transformation logic here
    uberDF['tpep_pickup_datetime'] = pd.to_datetime(uberDF['tpep_pickup_datetime'])
    uberDF['tpep_dropoff_datetime'] = pd.to_datetime(uberDF['tpep_dropoff_datetime'])

        # Remove duplicates for dateTime_dim table
    dateTime_dim = uberDF[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates()

    # Create dateTime dimension table.
    dateTime_dim['pickup_time'] = dateTime_dim['tpep_pickup_datetime'].dt.hour
    dateTime_dim['pickup_day'] = dateTime_dim['tpep_pickup_datetime'].dt.day
    dateTime_dim['pickup_month'] = dateTime_dim['tpep_pickup_datetime'].dt.month
    dateTime_dim['pickup_year'] = dateTime_dim['tpep_pickup_datetime'].dt.year
    dateTime_dim['pickup_weekday'] = dateTime_dim['tpep_pickup_datetime'].dt.weekday
    dateTime_dim['drop_time'] = dateTime_dim['tpep_dropoff_datetime'].dt.hour
    dateTime_dim['drop_day'] = dateTime_dim['tpep_dropoff_datetime'].dt.day
    dateTime_dim['drop_month'] = dateTime_dim['tpep_dropoff_datetime'].dt.month
    dateTime_dim['drop_year'] = dateTime_dim['tpep_dropoff_datetime'].dt.year
    dateTime_dim['drop_weekday'] = dateTime_dim['tpep_dropoff_datetime'].dt.weekday

    # dataTime dimension table creation
    dateTime_dim['datetime_id'] = dateTime_dim.index

    dateTime_dim = dateTime_dim[['datetime_id','tpep_pickup_datetime','pickup_time','pickup_day','pickup_month','pickup_year','pickup_weekday',
                                'tpep_dropoff_datetime','drop_time','drop_day','drop_month','drop_year','drop_weekday']]

        # Remove duplicates for passengerCount_dim table
    passengerCount_dim = uberDF[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passengerCount_dim['passenger_count_id'] = passengerCount_dim.index
    passengerCount_dim = passengerCount_dim[['passenger_count_id','passenger_count']]


        # Remove duplicates for tripDistance_dim
    tripDistance_dim = uberDF[['trip_distance']].drop_duplicates().reset_index(drop=True)
    tripDistance_dim['tripDis_id'] = tripDistance_dim.index
    tripDistance_dim = tripDistance_dim[['tripDis_id','trip_distance']]

    # Remove duplicates from location_dim 
    pickupLocation_dim = uberDF[['pickup_longitude','pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickupLocation_dim['pickup_id'] = pickupLocation_dim.index

    dropLocation_dim = uberDF[['dropoff_longitude','dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropLocation_dim['dropoff_id'] = dropLocation_dim.index

    # Remove duplicates for the rateCode dim
    rateCode_dict = {
        1 : 'Standard Rate',
        2 : 'JFK',
        3 : 'Newark',
        4 : 'Nassau or Westchester',
        5 : 'Negotiated fare',
        6 : 'Pooling'
    }

    rateCode_dim = uberDF[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rateCode_dim['rateCode_name'] = rateCode_dim['RatecodeID'].map(rateCode_dict)
    rateCode_dim = rateCode_dim[['RatecodeID','rateCode_name']]
    rateCode_dim.rename(columns = {'RatecodeID':'rateCode_id'}, inplace=True)



        # Remove duplicates for the payment dim
    payment_dict = {
        1 : 'Credit Card',
        2 : 'Cash',
        3 : 'No Charge',
        4 : 'Dispute',
        5 : 'Unknown',
        6 : 'Voided Trip'
    }

    payment_dim = uberDF[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_dim['payment_type_names'] = payment_dim['payment_type'].map(payment_dict)


    # Rename columns
    uberDF.rename(columns={'RatecodeID':'rateCode_id','VendorID':'vendor_id'},inplace=True)

    # Fact Table
    trip_fact = uberDF\
    .merge(dateTime_dim, on = ['tpep_pickup_datetime','tpep_dropoff_datetime']) \
    .merge(passengerCount_dim, on = 'passenger_count') \
    .merge(tripDistance_dim, on = 'trip_distance') \
    .merge(pickupLocation_dim, on = ['pickup_longitude','pickup_latitude'])\
    .merge(dropLocation_dim, on = ['dropoff_longitude', 'dropoff_latitude']) \
    .merge(payment_dim, on = 'payment_type')\
    .merge(rateCode_dim, on = 'rateCode_id') \
    [['trip_id','vendor_id','datetime_id','passenger_count_id',\
    'tripDis_id','pickup_id','dropoff_id','rateCode_id','payment_type', 'store_and_fwd_flag',
    'fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']]

    trips_data = uberDF.merge(passengerCount_dim, on='passenger_count') \
             .merge(tripDistance_dim, on='trip_distance') \
             .merge(rateCode_dim, on='RatecodeID') \
             .merge(pickupLocation_dim, on=['pickup_longitude', 'pickup_latitude']) \
             .merge(dropLocation_dim, on=['dropoff_longitude', 'dropoff_latitude'])\
             .merge(dateTime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
             .merge(payment_dim, on='payment_type') \
             [['VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]

    return {"dateTime_dim":datetime_dim.to_dict(orient="dict"),
    "passengerCount_dim":passenger_count_dim.to_dict(orient="dict"),
    "tripDistance_dim":trip_distance_dim.to_dict(orient="dict"),
    "rateCode_dim":rate_code_dim.to_dict(orient="dict"),
    "pickupLocation_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropLocation_dim":dropoff_location_dim.to_dict(orient="dict"),
    "payment_dim":payment_type_dim.to_dict(orient="dict"),
    "trips_data":trips_data.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
