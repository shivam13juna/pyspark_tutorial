"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, upper, row_number, min, when, expr, asc
from pyspark import SparkConf

from dependencies.spark import start_spark
from dependencies import ColNamesV5
import sys

# DEV
SOURCE_PATH_HOTEL_CITY_MAPPING_DEV = "s3://feature-store/v2/_redshift_loadable_data/_redshift_dump/hotel_dim/latest/000"
SOURCE_PATH_HOTEL_DETAIL_DEV = "s3://aws-s3-sink-hotels-kafka/app/batch/output/cdm/hotels/dtl/date_part=2020-09-01/part-00199-tid-6706756782233042738-a5fbe034-6f25-4d36-85cd-bdd750d9bd17-2982-1.c000.snappy.parquet"
DESTINATION_PATH_HOTEL_EVENTS_DEV = "s3://feature-store/testing_sep18/"

# STAGING
SOURCE_PATH_HOTEL_CITY_MAPPING_STAGING = "s3://feature-store/v2/_redshift_loadable_data/_redshift_dump/hotel_dim/latest/000"
SOURCE_PATH_HOTEL_DETAIL_STAGING = "s3://aws-s3-sink-hotels-kafka/app/batch/output/cdm/hotels/dtl/date_part="
DESTINATION_PATH_HOTEL_EVENTS_STAGING = "s3://feature-store/v2/_redshift_loadable_data/staging/events-fact/event_date="

# PROD
SOURCE_PATH_HOTEL_CITY_MAPPING_PROD = "s3://feature-store/v2/_redshift_loadable_data/_redshift_dump/hotel_dim/latest/000"
SOURCE_PATH_HOTEL_DETAIL_PROD = "s3://aws-s3-sink-hotels-kafka/app/batch/output/cdm/hotels/dtl/date_part="
DESTINATION_PATH_HOTEL_EVENTS_PROD = "s3://feature-store/v2/_redshift_loadable_data/events-fact/event_date="


def main():
    """Main ETL script definition.

    :return: None
    """

    # if len(sys.argv) == 0:
    #     print("Environment flag missing. Target date missing. Shutting down")
    #     print("Usage: EventsDataProcessingParquetv4 $Date(YYYY-MM-DD) $productionEnvironment")
    #     sys.exit()
    # elif len(sys.argv) == 1:
    #     print("Environment flag missing. Setting to default.")
    #     targetDate, environment = sys.argv[0], "dev"
    # else:
    #     targetDate, environment = sys.argv[0], sys.argv[1]
    
    # print("Event date : " + targetDate)
    # print("Environment : " + environment)
    environment = "dev"
    targetDate = "2020-09-01"
    sconf = SparkConf().setAppName("HotelClickStream_data_processing_" + targetDate)

    if environment == "dev":
        sconf.setMaster("local")
        hotelCityPath, detailFilePath, outputPath = SOURCE_PATH_HOTEL_CITY_MAPPING_DEV, SOURCE_PATH_HOTEL_DETAIL_DEV, DESTINATION_PATH_HOTEL_EVENTS_DEV
    elif environment == "staging":
        hotelCityPath, detailFilePath, outputPath = SOURCE_PATH_HOTEL_CITY_MAPPING_STAGING, SOURCE_PATH_HOTEL_DETAIL_STAGING + targetDate, DESTINATION_PATH_HOTEL_EVENTS_STAGING + targetDate
    elif environment == "prod":
        hotelCityPath, detailFilePath, outputPath = SOURCE_PATH_HOTEL_CITY_MAPPING_PROD, SOURCE_PATH_HOTEL_DETAIL_PROD + targetDate, DESTINATION_PATH_HOTEL_EVENTS_PROD + targetDate
    else:
        print("Setting to dev environment for safety")
        hotelCityPath, detailFilePath, outputPath = SOURCE_PATH_HOTEL_CITY_MAPPING_DEV, SOURCE_PATH_HOTEL_DETAIL_DEV + targetDate, DESTINATION_PATH_HOTEL_EVENTS_DEV + targetDate
    
    sparkSession = SparkSession.builder\
    .config(conf = sconf)\
    .getOrCreate()

    # sparkSession.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    runJob(sparkSession, targetDate, hotelCityPath, detailFilePath, outputPath)
    print("Events data successfully loaded into:" + outputPath)
    sparkSession.stop()


def runJob(sparkSession, targetDate, hotelCityPath, detailFilePath, outputPath):

    hotelCityDF = getHotelCityData(sparkSession, hotelCityPath)

    rawData = sparkSession.read.parquet(detailFilePath)

    isLocusColumnPresent = "locus" in rawData.columns

    sourceDF = getHotelEventsSourceData(sparkSession, rawData, isLocusColumnPresent)

    # import sparkSession.implicits._
    # Processing for meta_fnnl_step = detail
    detailData = getDetailsProcessedData(sparkSession, sourceDF.filter(sourceDF.meta_fnnl_step == "detail"), isLocusColumnPresent).withColumn("meta_row_type", lit(0))  # Legacy of bad column selection

    detailDataFinal = joinHotelDim(detailData, hotelCityDF, sparkSession)

    writeToS3(detailDataFinal, targetDate, outputPath)

    # Clearing cache
    sparkSession.catalog.dropTempView("DETAIL")
    sparkSession.catalog.dropTempView("CPN_STATUS_FINAL")

    # Get data for all metafnnl_step not detail
    nonDetailData = getNonDetailData(sparkSession, sourceDF)

    # Processing data for meta_fnnl_step = listing
    listingData = getListingProcessedData(sparkSession, nonDetailData, isLocusColumnPresent).withColumn("meta_row_type", lit(0))  # Legacy of bad column selection

    listingDataFinal = joinHotelDim(listingData, hotelCityDF, sparkSession)

    writeToS3(listingDataFinal, targetDate, outputPath)

    # Clearing cache
    sparkSession.catalog.dropTempView("LISTING")

    # Processing data for all other meta_fnnl_step(s)
    nonDetailListingData = getOtherProcessedData(sparkSession, nonDetailData, isLocusColumnPresent).withColumn("meta_row_type", lit(0))  # Legacy of bad column selection

    nonDetailListingDataFinal = joinHotelDim(nonDetailListingData, hotelCityDF, sparkSession)

    writeToS3(nonDetailListingDataFinal, targetDate, outputPath)


def getHotelCityData(sparkSession, sourcePath):
    hotelDimDF = sparkSession.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "false")\
    .option("delimiter", "|")\
    .load(sourcePath)\
    .select("hotel_id", "city_code")\
    .withColumnRenamed("hotel_id", "mmt_hotel_id")\
    .withColumn("htl_city_code", upper(col("city_code")))\
    .dropDuplicates()

    return hotelDimDF
  

def getHotelEventsSourceData(sparkSession, rawData, flagIsLocusColumnPresent):

    rawData.createOrReplaceTempView("MMT")

    # Locus columns not present before 2019-10-16. Putting check in place

    if flagIsLocusColumnPresent:

        sourceCols = ColNamesV5.sourceColsV2.replace("\n", " ") 
        locusFields = ColNamesV5.locusMainCols.replace("\n", " ") 
        sourceDF = sparkSession.sql("""SELECT {0}, {1} from MMT""".strip().format(sourceCols, locusFields))

    else:
        sourceCols = ColNamesV5.sourceColsV1.replace("\n", " ")
        sparkSession.sql("""SELECT {} from MMT""".strip().format(sourceCols))

    return sourceDF
  

def getDetailsProcessedData(sparkSession, sourceDF, flagIsLocusColumnPresent):

    sourceDF.createOrReplaceTempView("DETAIL")

    tariffCols = ColNamesV5.detailTariffCols.replace("\n", " ") 
    detailFileExtraCols = ColNamesV5.nonDisplayCols.replace("\n", " ") 

    # Explode the Room tariff array "pd_htl_trff_srvr_ls"
    detailWithHotelTariffExploded = sparkSession.sql("""SELECT *, {0}, {1} FROM DETAIL lateral view outer explode(pd_htl_trff_srvr_ls) tr""".strip().format(tariffCols, detailFileExtraCols))

    detailWithHotelTariffExploded.createOrReplaceTempView("DETAIL")

    """
     Select the row with min priced room when 'pd_htl_lowest_trff' is present or absent .
     Also if present all subrows can have false value hence rank them and choose the min one
    """

    if "pd_htl_lowest_trff" in detailWithHotelTariffExploded.columns:
        detailWithMinPricedRoomTariff = sparkSession.sql("""
        SELECT * FROM 
        (SELECT *, row_number() OVER (partition by meta_rw_id ORDER BY min_priced_room asc) as row_num FROM
        (SELECT *, CASE when pd_htl_lowest_trff = True then 1*prc_sell_amt_w_tax
                   ELSE 1000 * prc_sell_amt_w_tax
                   END as min_priced_room
                   FROM DETAIL
                   )
                )tmp
                   WHERE row_num <=1
        """.strip())

    else:

        detailWithMinPricedRoomTariff = sparkSession.sql("""
                             SELECT
                              *
                             FROM (
                                SELECT
                                  *,
                                  ROW_NUMBER() OVER (partition by meta_rw_id ORDER BY prc_sell_amt_w_tax asc) as row_num
                                From DETAIL ) tmp
                             WHERE row_num <= 1
                          """.strip())
      
    detailWithMinPricedRoomTariff.createOrReplaceTempView("DETAIL")

    detailColsRenamed = ColNamesV5.detailRenamedCols.replace("\n", " ") 
    tariffDetailColsRenamed = ColNamesV5.tariffDetailRenamedCols.replace("\n", " ") 
    couponColsRenamed = ColNamesV5.couponRenamedCols.replace("\n", " ") 
    nonListingExtraCols = ColNamesV5.nonListingCols.replace("\n", " ") 
    couponColsPosexploded = ColNamesV5.couponColsPosexplodeTest.replace("\n", " ") 

    if flagIsLocusColumnPresent:
        locusRenamedCols = ColNamesV5.locusRenamedColsV2.replace("\n", " ")
    else:
        locusRenamedCols = ColNamesV5.locusRenamedColsV1.replace("\n", " ") 

    detailWithMinPricedRoomTariffAndBestCoupon = sparkSession.sql("""
        SELECT
        {0}, {1}, {2}, {3}, 
        posexplode_row_num
        FROM (
         SELECT
           * ,
           {4}, {5}
         FROM DETAIL
         lateral view outer posexplode(cpn_resp_trff_ls) AS posexplode_row_num, cp
         )
        """.strip().format(detailColsRenamed, tariffDetailColsRenamed, couponColsRenamed, locusRenamedCols, couponColsPosexploded, nonListingExtraCols))\
    .drop("cpn_resp_trff_ls")

    detailWithMinPricedRoomTariffAndBestCoupon.createOrReplaceTempView("DETAIL")

    cpnStatusOrderedPivot = detailWithMinPricedRoomTariffAndBestCoupon\
    .groupBy("meta_row_id")\
    .pivot("cpn_status")\
    .agg(min("posexplode_row_num"))\
    .withColumnRenamed("meta_row_id", "meta_row_id_x")\
    .withColumn("final_cpn_status_row_num", when(col("BEST_COUPON").isNotNull(), col("BEST_COUPON")).otherwise(when(col("OTHER_COUPON").isNotNull(), col("OTHER_COUPON")).otherwise(lit(None))))\
    .drop("null")\
    .drop("BEST_COUPON")\
    .drop("OTHER_COUPON")

    cpnStatusOrderedPivot.createOrReplaceTempView("CPN_STATUS_FINAL")

    detailFinalDF = sparkSession.sql("""
         SELECT
          *
         FROM DETAIL inner join CPN_STATUS_FINAL
         WHERE DETAIL.meta_row_id = CPN_STATUS_FINAL.meta_row_id_x AND DETAIL.posexplode_row_num = CPN_STATUS_FINAL.final_cpn_status_row_num
          OR DETAIL.meta_row_id = CPN_STATUS_FINAL.meta_row_id_x AND DETAIL.cpn_status IS NULL
       """.strip())\
    .drop("meta_row_id_x")\
    .drop("final_cpn_status_row_num")\
    .drop("posexplode_row_num")

    return detailFinalDF
  

def joinHotelDim(dataFrameEvents, dataFrameHotelDim, sparkSession): 

    return dataFrameEvents.join(dataFrameHotelDim, dataFrameEvents.hotel_id == dataFrameHotelDim.mmt_hotel_id, how="left_outer")
  

def writeToS3(dataFrameFinal, targetDate, destinationPath):

    dataFrameFinal\
    .withColumn("event_date", lit(targetDate).cast("date"))\
    .sort(asc("htl_city_code"))\
    .write\
    .partitionBy("browsing_date", "meta_fnnl_step")\
    .option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
    .mode('append')\
    .parquet(destinationPath)


def getNonDetailData(sparkSession, sourceDF): 
    # import sparkSession.implicits._

    nonDetailData = sourceDF.filter(sourceDF.meta_fnnl_step != "detail")

    nonDetailData.createOrReplaceTempView("MMT")

    hotelDisplayCols = ColNamesV5.displayCols.replace("\n", " ") 

    requestFileExtraCols = ColNamesV5.requestFileExtraCols.replace("\n", " ") 

    requestWithExplodedData = sparkSession.sql(
    """
        SELECT
            *
        FROM (
            SELECT
                *,
                {0},
                {1}
            FROM
                MMT m
                lateral view outer posexplode(pd_htl_dtl_disp) as pos,d
            )
    """.strip().format(hotelDisplayCols, requestFileExtraCols))

    requestWithExplodedData.createOrReplaceTempView("MMT")

    # Filter out Listing Events for which client side hotel id is missing
    filteredSourceDF = requestWithExplodedData.filter(when(col("meta_fnnl_step") == "listing", col("pd_htl_id_htdsp").isNotNull()).otherwise(lit(True)))                                         

    filteredSourceDF.createOrReplaceTempView("MMT")

    return filteredSourceDF


def getListingProcessedData(sparkSession, sourceDF, flagIsLocusColumnPresent): 

    columns = sparkSession.sql(
    """
         SELECT
          col.pd_htl_rank as pd_htl_rank
         FROM MMT lateral view outer explode(pd_htl_srvr_ls) ls
    """.strip()).columns

    if "pd_htl_rank" in columns:
        listingArrayCols = ColNamesV5.listingColsV2.replace("\n", " ") 
    else:
        listingArrayCols = ColNamesV5.listingColsV1.replace("\n", " ") 

    # "pd_htl_srvr_ls" Array has Listing related events
    listingExploded = sparkSession.sql(
    """
        SELECT
        *,
        {}
        FROM MMT lateral view outer explode(pd_htl_srvr_ls) ls
        where ls.col.pd_htl_id = pd_htl_id_htdsp

    """.strip().format(listingArrayCols))

    listingExploded.createOrReplaceTempView("LISTING")

    detailColsRenamed = ColNamesV5.detailRenamedCols.replace("\n", " ") 
    tariffListingColsRenamed = ColNamesV5.tariffListingRenamedCols.replace("\n", " ") 
    couponColsRenamed = ColNamesV5.couponRenamedCols.replace("\n", " ") 
    couponColsPosexploded = ColNamesV5.couponColsPosexplodeTest.replace("\n", " ") 

    if flagIsLocusColumnPresent:
        locusRenamedCols = ColNamesV5.locusRenamedColsV2.replace("\n", " ")
    else:
        locusRenamedCols = ColNamesV5.locusRenamedColsV1.replace("\n", " ")

    # Explode cpn_resp_trff_ls and Filter BEST_COUPON
    listingExplodedWBestCoupon = sparkSession.sql("""
                                                        SELECT
                                                            {0}, {1}, {2}, {3},
                                                            posexplode_row_num
                                                        FROM (
                                                         SELECT
                                                           * ,
                                                           {4}
                                                         FROM LISTING
                                                         lateral view outer posexplode(ls_cpn_resp_htl_ls) AS posexplode_row_num, cp
                                                         )
                                                         WHERE cpn_status = 'RECOMMEND' OR  cpn_status IS NULL
                                                      """.strip().format(detailColsRenamed, tariffListingColsRenamed, couponColsRenamed, locusRenamedCols, couponColsPosexploded))\
    .drop("ls_cpn_resp_htl_ls")\
    .drop("cpn_resp_trff_ls")\
    .drop("posexplode_row_num")

    return listingExplodedWBestCoupon
  

def getOtherProcessedData(sparkSession, sourceDF, flagIsLocusColumnPresent):

    # import sparkSession.implicits._

    nonDetailData = sourceDF.filter(sourceDF.meta_fnnl_step != "detail").filter(sourceDF.meta_fnnl_step != "listing")

    nonDetailData.createOrReplaceTempView("MMT")

    detailColsRenamed = ColNamesV5.detailRenamedCols.replace("\n", " ")
    tariffNonListingColsRenamed = ColNamesV5.tariffNonListingRenamedCols.replace("\n", " ")
    couponColsRenamed = ColNamesV5.couponRenamedCols.replace("\n", " ")
    couponColsPosexploded = ColNamesV5.couponColsPosexplodeTest.replace("\n", " ")
    nonListingExtraCols = ColNamesV5.nonListingCols.replace("\n", " ")

    if flagIsLocusColumnPresent:
        locusRenamedCols = ColNamesV5.locusRenamedColsV2
    else:
        locusRenamedCols = ColNamesV5.locusRenamedColsV1

    requestWithExplodedDataAndBestCoupon = sparkSession.sql(
    """
            SELECT
            {0}, {1}, {2}, {3}, 
            posexplode_row_num
            FROM (
                SELECT
                *,
                {4},
                {5}
                FROM MMT
                lateral view outer posexplode(cpn_resp_ls) AS posexplode_row_num, cp
            )
    """.strip().format(detailColsRenamed, tariffNonListingColsRenamed, couponColsRenamed, locusRenamedCols, couponColsPosexploded, nonListingExtraCols)).drop("cpn_resp_trff_ls")

    requestWithExplodedDataAndBestCoupon.createOrReplaceTempView("NON_DETAIL_LISTING_EXPLODED")

    cpnStatusOrderedPivot = requestWithExplodedDataAndBestCoupon\
    .groupBy("meta_row_id").pivot("cpn_status")\
    .agg(min("posexplode_row_num"))\
    .withColumnRenamed("meta_row_id", "meta_row_id_x")\
    .withColumn("final_cpn_status_row_num", when(col("BEST_COUPON").isNotNull(), col("BEST_COUPON"))
    .otherwise(when(col("REDEEMED").isNotNull(), col("REDEEMED"))
        .otherwise(when(col("OTHER_COUPON").isNotNull(), col("OTHER_COUPON"))
        .otherwise(lit(None)))))\
    .select("meta_row_id_x", "final_cpn_status_row_num")

    cpnStatusOrderedPivot.createOrReplaceTempView("CPN_STATUS_FINAL")

    nonDetailListingFinalDF = sparkSession.sql(
    """
        SELECT
        *
        FROM NON_DETAIL_LISTING_EXPLODED inner join CPN_STATUS_FINAL
        WHERE NON_DETAIL_LISTING_EXPLODED.meta_row_id = CPN_STATUS_FINAL.meta_row_id_x AND NON_DETAIL_LISTING_EXPLODED.posexplode_row_num = CPN_STATUS_FINAL.final_cpn_status_row_num
        OR NON_DETAIL_LISTING_EXPLODED.meta_row_id = CPN_STATUS_FINAL.meta_row_id_x AND NON_DETAIL_LISTING_EXPLODED.cpn_status IS NULL
    """.strip())\
    .drop("meta_row_id_x")\
    .drop("final_cpn_status_row_num")\
    .drop("posexplode_row_num")

    return nonDetailListingFinalDF


def applySchema(dataFrame, sparkSession):
    dataFrame.createOrReplaceTempView("UNFORMATTED_DF")

    SQLQuery = EventsSchemaV5.typeCastsColumns(EventsSchemaV5.schemaV5) + "\nFROM UNFORMATTED_DF"

    return sparkSession.sql(SQLQuery.stripMargin)
  

  
    # start Spark application and get Spark session, logger and config
    # spark, log, config = start_spark(
    #     app_name='my_etl_job',
    #     files=['configs/etl_config.json'])

    # # log that main ETL job is starting
    # log.warn('etl_job is up-and-running')

    # # execute ETL pipeline
    # data = extract_data(spark)
    # data_transformed = transform_data(data, config['steps_per_floor'])
    # load_data(data_transformed)

    # # log the success and terminate Spark application
    # log.warn('test_etl_job is finished')
    # spark.stop()
    # return None


# def extract_data(spark):
#     """Load data from Parquet file format.

#     :param spark: Spark session object.
#     :return: Spark DataFrame.
#     """
#     df = (
#         spark
#         .read
#         .parquet('tests/test_data/employees'))

#     return df


# def transform_data(df, steps_per_floor_):
#     """Transform original dataset.

#     :param df: Input DataFrame.
#     :param steps_per_floor_: The number of steps per-floor at 43 Tanner
#         Street.
#     :return: Transformed DataFrame.
#     """
#     df_transformed = (
#         df
#         .select(
#             col('id'),
#             concat_ws(
#                 ' ',
#                 col('first_name'),
#                 col('second_name')).alias('name'),
#                (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

#     return df_transformed


# def load_data(df):
#     """Collect data locally and write to CSV.

#     :param df: DataFrame to print.
#     :return: None
#     """
#     (df
#      .coalesce(1)
#      .write
#      .csv('loaded_data', mode='overwrite', header=True))
#     return None


# def create_test_data(spark, config):
#     """Create test data.

#     This function creates both both pre- and post- transformation data
#     saved as Parquet files in tests/test_data. This will be used for
#     unit tests as well as to load as part of the example ETL job.
#     :return: None
#     """
#     # create example data from scratch
#     local_records = [
#         Row(id=1, first_name='Dan', second_name='Germain', floor=1),
#         Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
#         Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
#         Row(id=4, first_name='Ken', second_name='Lai', floor=2),
#         Row(id=5, first_name='Stu', second_name='White', floor=3),
#         Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
#         Row(id=7, first_name='Phil', second_name='Bird', floor=4),
#         Row(id=8, first_name='Kim', second_name='Suter', floor=4)
#     ]

#     df = spark.createDataFrame(local_records)

#     # write to Parquet file format
#     (df
#      .coalesce(1)
#      .write
#      .parquet('tests/test_data/employees', mode='overwrite'))

#     # create transformed version of data
#     df_tf = transform_data(df, config['steps_per_floor'])

#     # write transformed version of data to Parquet
#     (df_tf
#      .coalesce(1)
#      .write
#      .parquet('tests/test_data/employees_report', mode='overwrite'))

#     return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
