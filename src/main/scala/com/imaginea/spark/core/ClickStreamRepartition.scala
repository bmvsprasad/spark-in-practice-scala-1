package com.imaginea.spark.core

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sampathr on 25/8/16.
  */
object ClickStreamRepartition {

  def main(args: Array[String]) {
    processData()
  }

  def processData() = {
    val conf = new SparkConf().setAppName("ClickStreamParquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputPath = "hdfs://localhost:54310/user/data/test/sample-s3.log"
    val outputPath = "hdfs://localhost:54310/user/data/demo/test1/12"
    val paritions = 3
    System.setProperty("spark.hadoop.dfs.replication", "1")
    val clicks = sc.textFile(inputPath)
    val rawSchema = """1_time,2_status,3_type,4_rotatorId,5_UUID,6_processingTime,7_IP,8_method,9_viewAspect,10_request,11_referrer,12_userAgent,13_secureRequest,14_optoutCookie,15_cookieEnabled,16_sessionId,17_country,18_region,19_dma,20_zip,21_ajkey,22_uniqueUser,23_zone,24_advertiser,25_campaign,26_banner,27_dimension,28_adgroup,29_publisher,30_adspot,31_page,32_channel,33_category,34_subcategory,35_geolimitUid,36_dimSublimit,37_adspotSublimit,38_rootAdspotSublimit,39_clickUrlId,40_commission,41_cost,42_keywords,43_customStats,44_flashEventCount,45_flashEventId,46_flashEventType,47_pctId,48_pctCustomer,49_pctAmount,50_pctOrder,51_pctAction,52_pctDetails,53_pctSource,54_trackedPage,55_city,56_emp,57_requestedKeywords,58_rtbCompetitive,59_rtbResponse,60_globalChannelAd,61_mobile,62_vast,63_mobileCarrier,64_mobileOS,65_mobileDevice,66_mobileVendor,67_latitude,68_longitude,69_predefinedMobileAreas,70_customMobileAreas,71_aj_aud_bk,72_rtbServer,73_mobileType,74_globalChannels,75_blockedDomain,76_landingUrl,77_ajBid,78_openrtbStatus,79_openrtbTime,80_openrtbConfPriceFloor,81_openrtbActualPriceFloor,82_openrtbBid,83_openrtbSource,84_openrtbExtID,85_supplyCost,86_openrtbStep,87_openrtbWin,88_videoView,89_siteDomain,90_viewability,91_width,92_height,93_sdkVersion,94_deviceId,95_userId,96_parentUUID,97_agency,98_dlkSegments,99_adUnit,100_mobileOSVersion,101_mobileSdkVersion,102_mobileOwnerName,103_mobileResolutionWidth,104_mobileResolutionHeight,105_mobilePhysicalScreenWidth,106_mobilePhysicalScreenHeight,107_language,108_coppaEnabled,109_adMediaType,110_vastProtocols,111_vastLinearity,112_vastAdType,113_incomingOpenRtb,114_urlDomain,115_referDomain,116_domainDetectionSource,117_parentDimension,118_vmDeploy,119_vmMode,120_vmAttempt,121_flight,122_defaultAdType,123_originalTime,124_originalTimeOffsetMs"""
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

    clicksDF.repartition(5).write.partitionBy("1_time").parquet("")
    clicksDF.write.partitionBy("1_time").parquet(outputPath)
    //clicksDF.coalesce(paritions.toInt).write.parquet(outputPath)
    val netTime = System.currentTimeMillis() - startOfNetTime
    println("Sampath")
    print(netTime)
  }
}

