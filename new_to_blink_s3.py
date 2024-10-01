from utils.utils import bq_to_pd_v2
from utils.s3_utils import S3
import pendulum
from utils.setting import AWS_PROD_SERVER_PUBLIC_KEY, AWS_PROD_SERVER_SECRET_KEY
# import os
# os.environ()

    # AND ods.transaction_date < "{nxt_observation_date}"

observation_date = pendulum.now().to_date_string()
joinable_date = pendulum.now().subtract(days=1).to_date_string()
nxt_observation_date = pendulum.parse(observation_date).add(days=1).to_date_string()
q = f'''
-- 30 days within shell
-- transaction table participants id == 1

WITH ranked_transactions AS (
  SELECT 
    ods.transaction_id, 
    ods.card_no, 
    ods.total_txn_value,
    ods.transaction_date, 
    blm.assign_date,
    app.createddateutc,
    DATE_DIFF(DATE(ods.transaction_date), DATE(app.createddateutc), DAY) AS app_day_diff,
    DATE_DIFF(DATE(ods.transaction_date), DATE(blm.assign_date), DAY) AS blm_day_diff,
    ROW_NUMBER() OVER (PARTITION BY ods.card_no ORDER BY ods.transaction_id ASC) AS row_num,
    blm.mobile,
    blm.mobile_original,
    blm.first_name as name,
    blm.email,
    ods.partition_dt
  FROM 
    `blink-data-warehouse.base_layer.ods_tx_txn_df` ods
  LEFT JOIN 
    `blink-data-warehouse.aggregate_layer.dws_etl_cd_card_df` blm
  ON 
    CAST(TRIM(ods.card_no) AS INT) = CAST(blm.card_no AS INT)
  LEFT JOIN 
    `blink-data-warehouse.aggregate_layer.dws_etl_mobileapp2_blmember_df` app
  ON
    CAST(TRIM(ods.card_no) AS INT) = app.blcard
  WHERE
    ods.partition_dt >= "2024-07-15"
    AND TRIM(tx_type_code) IN ("0", "4")
    AND app.createddateutc >= "2024-07-22"
    AND ods.transaction_date >= "2024-07-22"
    AND DATE_DIFF(DATE(ods.transaction_date), DATE(app.createddateutc), DAY) <= 30
    AND DATE_DIFF(DATE(ods.transaction_date), DATE(blm.assign_date), DAY) <= 30
    AND ods.total_txn_value >= 30
)

SELECT 
  transaction_id, 
  card_no, 
  total_txn_value,
  transaction_date, 
  assign_date as registration_date, 
  createddateutc as app_install_date,
  app_day_diff,
  blm_day_diff,
  mobile,
  mobile_original,
  name,
  email,
  partition_dt
FROM
  ranked_transactions
WHERE 
  row_num = 1
'''

df = bq_to_pd_v2(q)

#trim card_no and convert it to int
df['card_no'] = df['card_no'].str.strip()
df['card_no'] = df['card_no'].astype(int)
#fillna mobile with mobile_original
df['mobile'] = df['mobile'].fillna(df['mobile_original'])
df.drop(columns=['mobile_original', 'email'], inplace=True)

#strip leading or trailing spaces tabs from string columns
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df["partition_dt"] = df["partition_dt"].astype(str)
df = df[df["partition_dt"] == observation_date]

#SENSITIVE ASSIGNMENT#!!!!!!!!!!!!!
region_name = "ap-southeast-1"
#SENSITIVE ASSIGNMENT ENDED#!!!!!!!!!!!!!
s3_path = f"bonuslink"
bucket_name = 'bonuslink-production-partners-points-raw'
#compare s3 tables latest entries

s3 = S3(AWS_PROD_SERVER_PUBLIC_KEY, AWS_PROD_SERVER_SECRET_KEY, region_name = region_name, staging=False)
s3.upload_df_to_s3(df, bucket_name, s3_path + f"/{observation_date}/shell-500_1.csv")
print(f"completed:: {observation_date}")

