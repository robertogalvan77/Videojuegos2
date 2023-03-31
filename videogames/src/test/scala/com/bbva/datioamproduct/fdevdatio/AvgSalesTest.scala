package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.VideoGamesConfigConstants.FdevGamesSales
import com.bbva.datioamproduct.fdevdatio.common.filds.JaponSales
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.VideoGamesConfig
import org.apache.spark.sql.{AnalysisException, DataFrame}
import com.bbva.datioamproduct.fdevdatio.transformations.DfTransformations

class AvgSalesTest extends  ContextProvider{
  //Prueba de éxito cuando el método avgSales se ejecuta
  "avgSales method " should "finish a succeed execution" in{
    val dfMap: Map[String, DataFrame] =config.readInputs()
    val df : DataFrame = dfMap(FdevGamesSales)
    df.avgSales.show()
    succeed
  }
  //Prueba de falla cuando el método avgSales le falta una columna
  "avgSales method " should "throw a AnalysisException because of missing column" in {
    assertThrows[AnalysisException] {
      val dfMap: Map[String, DataFrame] = config.readInputs()
      val df: DataFrame = dfMap(FdevGamesSales)
      df.select(JaponSales.column).avgSales.show()
    }
  }
}
