from jobs.etl_job import getHotelEventsSourceData, getHotelCityData, joinHotelDim, getNonDetailData
from dependencies import ColNamesV5
from jobs.etl_job import getDetailsProcessedData   
from pyspark.sql.functions import *


hotelCityDF = getHotelCityData(spark, "data/hotel_dim/latest/000")
into = spark.read.parquet("data/hotel_dtl/198.parquet")
sourceDF = getHotelEventsSourceData(spark, into, True) 

# sourceDF.createOrReplaceTempView("DETAIL")
# check = getDetailsProcessedData(spark, sourceDF, True)

# detailDataFinal = joinHotelDim(check, hotelCityDF, spark)

nonDetailData = getNonDetailData(spark, sourceDF)


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

requestWithExplodedDataAndBestCoupon = spark.sql(
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
