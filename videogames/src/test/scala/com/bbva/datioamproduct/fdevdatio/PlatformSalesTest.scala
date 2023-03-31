package com.bbva.datioamproduct.fdevdatio


import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.VideoGamesConfig
import org.apache.spark.sql.{AnalysisException, DataFrame}
import com.bbva.datioamproduct.fdevdatio.transformations.DfTransformations
import com.bbva.datioamproduct.fdevdatio.transformations.MapToDataFrame
import org.apache.spark.sql.functions.col

class PlatformSalesTest extends  ContextProvider{
  //Prueba de éxito cuando el método  platformSales se ejecuta
  "platformSales method " should "finish a succeed execution" in {
    val df: DataFrame = config.readInputs().getFullDF
    df
      .platformSales
      .show(30)
    succeed
  }
  //Prueba de falla cuando el  platformSales cuando existen columnas iguales
  "platformSales method " should "throw a AnalysisException because ambiguos" in {
    assertThrows[AnalysisException] {
      val df: DataFrame = config.readInputs().getFullDF
      df.select(col("cutoff_date"))
        .platformSales
        .show(30)
    }
  }

}
