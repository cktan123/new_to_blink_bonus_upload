from utils.utils import bq_to_pd_v2
import pendulum
import pandas as pd
import numpy as np
import json

def process_tx_txn(yesterday, batch, transaction_id_min, transaction_id_max, groupcode_df, productcode_df, pii_df, outlet_location_info_df):
    interested_cols = [
        "ods.partition_dt", 
        "ods.transaction_id", 
        "ods.card_no", 
        "ods.terminal_id",
        "ods.transaction_date", 
        "ods.total_txn_value", 
        "ods.std_points_value", 
        "ods.bonus_points_value", 
        "ods.source", 
        "ods.merch_ref", 
        "ods.statement_id", 
        "ods.name", 
        "ods.card_type",
        'ods.form_of_pmt',
        'tx_type_code',
        "etl_dt.product_code",
        "etl_dt.group_code",
        "etl_dt.qty",
        "etl_dt.value",
        "etl_dt.std_pts",
        "etl_dt.bonus_pts"
        ]

    #combine as string seperate with comma
    interested_cols = ", ".join(interested_cols)

    query =\
        f"""
        SELECT
            {interested_cols}
        FROM `blink-data-warehouse.base_layer.ods_tx_txn_df` ods
        LEFT JOIN `blink-data-warehouse.base_layer.etl_tx_txn_detail` etl_dt
        ON ods.transaction_id = etl_dt.transaction_id
            WHERE partition_dt = "{yesterday}"
            AND TRIM(tx_type_code) IN ("0", "4")
            AND ods.transaction_id >= {transaction_id_min}
            AND ods.transaction_id < {transaction_id_max}
        """
    df = bq_to_pd_v2(query)
    #check if df is empty, if empty return None
    if df.empty:
        print(f"completed batch {batch} with null entry, nothing will be written")
        return pd.DataFrame()

    #join df with productcode and groupcode for addtional information
    #convert column type to match int
    df["group_code"] = pd.to_numeric(df["group_code"], errors='coerce')
    df["product_code"] = pd.to_numeric(df["product_code"], errors='coerce')
    df['card_no'] = df['card_no'].str.strip()
    df["card_no"] = df["card_no"].astype(int)
    #strip the 
    df["terminal_id"] = df["terminal_id"].str.strip()

    df = df.merge(groupcode_df, how="left", on="group_code")
    # df = df.merge(productcode_df, how="left", on="product_code")
    df = df.merge(pii_df, how="left", on="card_no")
    df = df.merge(outlet_location_info_df, how="left", on='terminal_id')

    #tx_type, if tx_type = 0 thn it is 'issuance' if tx_type = 4 thn it is 'online_issuance' else 'unlabeled'
    df["tx_type"] = 'issue'

    _ = df[["transaction_id", "product_code", "group_code", "qty", 'std_pts', 'bonus_pts', 'value', 'std_points_value','bonus_points_value', "email", "mobile"]].copy()
    _["userId"] = _["email"].fillna(_["mobile"]).infer_objects()
    #if userId is not null, label it as email
    _["userId_type"] = np.where(_['email'].notna() & _['mobile'].notna(), 
                            'email',
                            np.where(_['email'].notna(), 'email', 
                                    np.where(_['mobile'].notna(), 'mobile', np.nan)))


    _['transaction_id'] = _['transaction_id'].astype(str)
    _["product_code"] = pd.to_numeric(_["product_code"], errors='coerce')
    _["group_code"] = _["group_code"].astype('str')
    _["value"] = pd.to_numeric(_["value"], errors='coerce')
    _["std_points_value"] = _["std_points_value"].astype(float)
    _["bonus_points_value"] = _["bonus_points_value"].astype(float)
    _['std_pts'] = pd.to_numeric(_["std_pts"], errors='coerce').astype(float)
    _['bonus_pts'] = pd.to_numeric(_["bonus_pts"], errors='coerce').astype(float)

    _["email"] = _["email"].fillna("")
    _["mobile"] = _["mobile"].fillna("")
    _["product_code"] = _["product_code"].fillna(0)
    #group_code replace nan with empty string
    _["group_code"] = _["group_code"].fillna("")
    _["qty"] = _["qty"].fillna(0).astype(float)
    _["std_pts"] = _["std_pts"].fillna(0)
    _["bonus_pts"] = _["bonus_pts"].fillna(0)
    _["value"] = _["value"].fillna(0)
    _["std_points_value"] = _["std_points_value"].fillna(0).astype(float)
    _["bonus_points_value"] = _["bonus_points_value"].fillna(0).astype(float)
    _["userId"] = _["userId"].fillna("")
    _["userId_type"] = _["userId_type"].fillna("")

    #strip tabs and leading and trailing tabs
    _["group_code"] = _['group_code'].str.strip()
    _["email"] = _['email'].str.strip()
    _["mobile"] = _['mobile'].str.strip()
    _["userId"] = _['userId'].str.strip()
    _["userId_type"] = _['userId_type'].str.strip()
    _["group_code"] = _['group_code'].replace('nan', '')

    # Group by and aggregate the columns into lists
    grouped = _.groupby(['transaction_id']).agg(list).reset_index()

    # tmp = grouped.groupby("userId").agg({'userId_type': 'max'}).reset_index()

    # Merge the temporary DataFrame with the original grouped DataFrame to retain all rows
    #Create the JSON-like column for userId
    grouped['user'] = grouped.apply(
        lambda row: {
            'id': max(row['userId']),
            'type': max(row['userId_type'])
        }, axis=1)
    grouped['user'] = grouped['user'].apply(lambda x: json.dumps(x).replace('"','\\"'))
        
    # Create the JSON-like column
    grouped['product_points'] = grouped.apply(
        lambda row: [
            {'standard': std_pts, 'bonus': bonus_pts} 
            for std_pts, bonus_pts in 
                    zip(
                        row['std_pts'],
                        row["bonus_pts"],
                        )
                    ], axis=1)

    # Create the JSON-like column
    grouped['products'] = grouped.apply(
        lambda row: [
            {'amount':value, "categoryCode":group_code, 'points':product_points, 'productCode': product_code, 'quantity':qty} 
            for value, group_code, product_points, product_code, qty in 
                    zip(
                        row['value'],
                        row['group_code'],
                        row["product_points"],
                        row['product_code'],
                        row['qty'],
                        )
                    ], axis=1)
    grouped['products'] = grouped['products'].apply(lambda x: json.dumps(x).replace('"','\\"'))

    grouped['gateway'] = grouped.apply(
        lambda row: [
            {'id': 1, 'transactionId': transaction_id} 
            for transaction_id 
            in [row["transaction_id"]]
        ], axis=1)
    grouped['gateway'] = grouped['gateway'].apply(lambda x: x[0] if len(x) > 0 else None)
    grouped['gateway'] = grouped['gateway'].apply(lambda x: json.dumps(x).replace('"','\\"'))

    grouped['points'] = grouped.apply(
        lambda row: {
            "standard": max(row['std_points_value']),
            "bonus": max(row['bonus_points_value'])
        }, axis=1)
    grouped['points'] = grouped['points'].apply(lambda x: json.dumps(x).replace('"','\\"'))

    product_gateway_out = grouped[["transaction_id","products", 'gateway', 'points', 'user']].copy()
    product_gateway_out["transaction_id"] = product_gateway_out["transaction_id"].astype(str)
    #columns needed
    out_cols = ["transaction_id", "card_no", "total_txn_value", "std_points_value", 
                "bonus_points_value", 'merch_ref', 'participant_name', "form_of_pmt", 'transaction_date', 
                "terminal_id", 'tx_type', "latitude", 'longitude']


    _2 = df[out_cols].drop_duplicates()
    _2["transaction_id"] = _2["transaction_id"].astype(str)
    _2 = _2.merge(product_gateway_out, how="left", on="transaction_id")
    _2 = _2[["card_no", 'user', "total_txn_value", "gateway", "transaction_date", "merch_ref", 
        "participant_name", "form_of_pmt", "points", "products", "terminal_id", "tx_type", "latitude", "longitude"]]

    _2["merch_ref"] = _2["merch_ref"].str.strip()
    _2["participant_name"] = _2["participant_name"].str.strip()
    _2["form_of_pmt"] = _2["form_of_pmt"].str.strip()
    _2["terminal_id"] = _2["terminal_id"].str.strip()
    _2["tx_type"] = _2["tx_type"].str.strip()

    #correct format
    # _2["total_txn_value"] = _2["total_txn_value"].astype('float')
    # _2["std_points_value"] = _2["std_points_value"].astype('int64')
    # _2["bonus_points_value"] = _2["bonus_points_value"].astype('int64')
    _2["participant_name"] = _2["participant_name"].fillna("")
    _2["latitude"] = _2["latitude"].fillna(0)
    _2["longitude"] = _2["longitude"].fillna(0)

    #rename all to match the right column names
    _2.rename(columns={
        "card_no":"cardId", 
        "total_txn_value":"amount", 
        "transaction_date":"issuedAt",
        'merch_ref':'merchantReference',
        'participant_name':'partner',
        'form_of_pmt': 'paymentMode',
        'terminal_id':'terminalId',
        'tx_type': 'type'
        },inplace=True)

    return _2

    # #upload to s3
    # # Define your S3 bucket name and the object name (file name)
    # reverse_batch = max_batch - batch
    # s3_path = f's3://{bucket}/{s3_path}/year={year}/month={month}/day={day}/{reverse_batch}.csv'

    # # Write DataFrame to S3
    # #initiate S3 Instance
    # s3 = S3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION_NAME, staging=True, log_level=logging.CRITICAL)
    # _2.to_csv(s3_path, index=False, quotechar='\'', quoting=csv.QUOTE_NONE, escapechar='\\', storage_options={
    #     'key': AWS_ACCESS_KEY_ID,
    #     'secret': AWS_SECRET_ACCESS_KEY,
    #     'token': AWS_SESSION_TOKEN,
    #     'client_kwargs': {'region_name': AWS_REGION_NAME}
    # })
    # print(f"completed date:: {yesterday}, batch:: {batch}")
   

#joinable tables function
def get_all_joinable(joinable_yesterday):
    #add all joinable tables first
    q = '''
        SELECT 
            pp.participant_id,
            trim(pp.description) participant_name,
            pp1.participant_id outlet_id,
            trim(pp1.description) outlet_name,
            trim(pt.terminal_id) terminal_id
        FROM base_layer.pt_participant pp
        INNER JOIN base_layer.pt_participant AS pp1 ON pp.participant_id = pp1.parent_id --outlet
        INNER JOIN base_layer.pt_participant AS pp2 ON pp1.participant_id = pp2.parent_id 
        INNER JOIN base_layer.pt_terminal pt ON pp2.participant_id = pt.participant_id
        where pp._PARTITIONDATE = current_date()
        and pp1._PARTITIONDATE = current_date()
        and pp2._PARTITIONDATE = current_date()
        and pt._PARTITIONDATE = current_date()
        and pt.terminal_id not in ('SHVPTS01', 'SHVPTS02', 'SHVPTS03', 'SHVPTS04', 'SHVPTS05', 'SHVPTS06', 'SHVPTS07', 'SHVPTS08', 'SHVPTS09', 'SHVPTS10')
        '''
        
    outlet_id_df = bq_to_pd_v2(q)
    outlet_id_df["terminal_id"] = outlet_id_df["terminal_id"].str.strip()

    q = f"""
        SELECT outletid as outlet_id, latitude, longitude FROM `blink-data-warehouse.base_layer.etl_mobileapp2_outlet` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("{joinable_yesterday}")
        """
    outlet_location_info_df = bq_to_pd_v2(q)

    outlet_location_info_df = outlet_location_info_df.merge(outlet_id_df, on="outlet_id", how="left")
    #drop outlet_id_df no longer useful
    del outlet_id_df

    #groupcode_df
    q = f'''
        SELECT * FROM `blink-data-warehouse.base_layer._Shell_ref_groupcode` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP('{joinable_yesterday}')
        '''
    groupcode_df = bq_to_pd_v2(q)[["group_code", "category", "sub_category", "product_type"]]
    groupcode_df["group_code"] = pd.to_numeric(groupcode_df["group_code"], errors='coerce')
    groupcode_df.dropna(subset=["group_code"],inplace=True)
    groupcode_df["group_code"] = groupcode_df["group_code"].astype(int)

    productcode_df = None
    q = f'''
        SELECT card_no, email, mobile 
        FROM `blink-data-warehouse.base_layer.nc_contact_base` 
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("{joinable_yesterday}") 
        AND (email IS NOT NULL OR mobile IS NOT NULL);
    '''
    pii_df = bq_to_pd_v2(q)
    pii_df["card_no"] = pii_df["card_no"].str.strip()
    pii_df["card_no"] = pii_df["card_no"].astype(int)

    return groupcode_df, productcode_df, pii_df, outlet_location_info_df

def generate_date_strings(start_date_str, end_date_str, fmt="YYYY-MM-DD"):
    start_date = pendulum.parse(start_date_str)
    end_date = pendulum.parse(end_date_str)

    # Create a list to hold the date strings
    date_list = []

    # Use pendulum's range method to iterate over the period
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.format(fmt))
        current_date = current_date.add(days=1)

    return date_list