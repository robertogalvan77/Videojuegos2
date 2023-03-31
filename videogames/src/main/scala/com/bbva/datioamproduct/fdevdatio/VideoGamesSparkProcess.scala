package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.NumericConfigConstans.Zero
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.VideoGamesConfigConstants.{FdevGamesInfoTag, FdevGamesSales}
import com.bbva.datioamproduct.fdevdatio.common.filds.{Clasification, CompleteName, VideoGameGenre}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, VideoGamesConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import com.bbva.datioamproduct.fdevdatio.transformations.{DfTransformations, MapToDataFrame}
import com.bbva.datioamproduct.fdevdatio.common.filds._


class VideoGamesSparkProcess extends SparkProcess with IOUtils{
  private val logger:Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("Hola este es el ejercicio")
    val config:Config = runtimeContext.getConfig
    val dfMap:Map[String, DataFrame]= config.readInputs()
    // ************************************************************************//
    // ********************  F U N C I O N 1  *********************************//
    // ************************************************************************//


//    val dfAvg:DataFrame = dfMap(FdevGamesSales)
//    dfAvg.avgSales.show()

    // ************************************************************************//
    // ********************  F U N C I O N 2  *********************************//
    // ************************************************************************//

//    val dfPlatformSales: DataFrame = dfMap.getFullDF
//    dfPlatformSales
//      .platformSales
//      .show(30)

    // ************************************************************************//
    // ********************  F U N C I O N 3  *********************************//
    // ************************************************************************//

//
   val dfJoinInfoSales=dfMap.getFullDF
    dfJoinInfoSales.topVideoGames.na.fill(0).show

    // ************************************************************************//
    // ********************  F U N C I O N 4  *********************************//
    // ************************************************************************//

//    val dfSalesGlobalByConsole: DataFrame = dfMap.getGlobalFullDF
//    dfSalesGlobalByConsole.show(30)
//    println("Registros Ventas Consolas"+globalDatasF.count())

    // ************************************************************************//
    // ********************  F U N C I O N 5  *********************************//
    // ************************************************************************//

//    val df1:DataFrame = dfMap(FdevGamesInfoTag).drop(CutoffDate.column)
//    val dfClasi: DataFrame = df1.addColumn(CompleteName())
//      .addColumn(Clasification())
//    dfClasi.show()

   // ************************************************************************/ /
    // ********************  F U N C I O N 6  *********************************//
    // ************************************************************************//

//   val df1: DataFrame = dfMap(FdevGamesInfoTag)
//     .drop(CutoffDate.column)
//     .drop(NaSalesPer.column)
//     .drop(OtherSalesPer.column)
//    val dfSalesGlobalByConsole: DataFrame = dfMap.getGlobalFullDF
//    val dfClasi: DataFrame = df1.addColumn(CompleteName())
//      .addColumn(Clasification())
//    dfClasi.show()
//    val dfALL: DataFrame =dfClasi.getFinal(dfSalesGlobalByConsole,dfClasi)
//
//    dfALL.show(30)
//    println(dfALL.count())

    Zero
  }

  override def getProcessId: String = "VideoGameProcess"
}
