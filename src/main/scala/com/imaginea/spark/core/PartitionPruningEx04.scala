package com.imaginea.spark.core

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sampathr on 26/8/16.
  */
object PartitionPruningEx04 {
  def main(args: Array[String]) {
    processData()
  }

  def processData() = {
    val conf = new SparkConf().setAppName("ClickStreamParquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputPath = "hdfs://localhost:54310/user/data/test/random-data.txt"
    val outputPath = "hdfs://localhost:54310/user/data/demo/random-data-countryPartitioned"
    System.setProperty("spark.hadoop.dfs.replication", "1")
    val clicks = sc.textFile(inputPath)
    val rawSchema = """1_date,2_hour,3_status,4_type,5_rotatorId,6_UUID,7_processingTime,8_IP,9_method,10_viewAspect,11_request,12_referrer,13_userAgent,14_secureRequest,15_optoutCookie,16_cookieEnabled,17_sessionId,18_country,19_region,20_dma,21_zip,22_ajkey,23_uniqueUser,24_zone,25_advertiser,26_campaign,27_banner,28_dimension,29_adgroup,30_publisher,31_adspot,32_page,33_channel,34_category,35_subcategory,36_geolimitUid,37_dimSublimit,38_adspotSublimit,39_rootAdspotSublimit,40_clickUrlId,41_commission,42_cost,43_keywords,44_customStats,45_flashEventCount,46_flashEventId,47_flashEventType,48_pctId,49_pctCustomer,50_pctAmount,51_pctOrder,52_pctAction,53_pctDetails,54_pctSource,55_trackedPage,56_city,57_emp,58_requestedKeywords,59_rtbCompetitive,60_rtbResponse,61_globalChannelAd,62_mobile,63_vast,64_mobileCarrier,65_mobileOS,66_mobileDevice,67_mobileVendor,68_latitude,69_longitude,70_predefinedMobileAreas,71_customMobileAreas,72_aj_aud_bk,73_rtbServer,74_mobileType,75_globalChannels,76_blockedDomain,77_landingUrl,78_ajBid,79_openrtbStatus,80_openrtbTime,81_openrtbConfPriceFloor,82_openrtbActualPriceFloor,83_openrtbBid,84_openrtbSource,85_openrtbExtID,86_supplyCost,87_openrtbStep,88_openrtbWin,89_videoView,90_siteDomain,91_viewability,92_width,93_height,94_sdkVersion,95_deviceId,96_userId,97_parentUUID,98_agency,99_dlkSegments,100_adUnit,101_mobileOSVersion,102_mobileSdkVersion,103_mobileOwnerName,104_mobileResolutionWidth,105_mobileResolutionHeight,106_mobilePhysicalScreenWidth,107_mobilePhysicalScreenHeight,108_language,109_coppaEnabled,110_adMediaType,111_vastProtocols,112_vastLinearity,113_vastAdType,114_incomingOpenRtb,115_urlDomain,116_referDomain,117_domainDetectionSource,118_parentDimension,1198_vmDeploy,120_vmMode,121_vmAttempt,122_flight,123_defaultAdType,124_originalTime,125_originalTimeOffsetMs"""
    val schemaFields = rawSchema.split(",").map { x: String => x.trim() }
    val schema =
      StructType(
        schemaFields.map(fieldName => StructField(fieldName, StringType, true)))
    val sqlContext = new SQLContext(sc)
    val clicksDF = sqlContext.createDataFrame(clicks.map { line =>
      val fields = line.split("\t")
      Row.fromSeq(fields)
    }, schema)

    val startOfNetTime = System.currentTimeMillis()
    clicksDF.repartition(clicksDF("18_country")).write.partitionBy("18_country").parquet(outputPath)
    // val paritions = 3
    // clicksDF.coalesce(paritions.toInt).write.partitionBy("18_country").parquet(outputPath)
    val netTime = System.currentTimeMillis() - startOfNetTime
    println(netTime)
  }
}
