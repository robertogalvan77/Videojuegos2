package com.bbva.datioamproduct.fdevdatio


import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.VideoGamesConfig
import org.apache.spark.sql.{DataFrame}
import com.bbva.datioamproduct.fdevdatio.transformations.MapToDataFrame
import com.bbva.datioamproduct.fdevdatio.transformations.JoinException


class GetGlobalFullDFTest extends ContextProvider{
  //Prueba de éxito cuando el método GetGlobalFullDF se ejecuta
  "GetGlobalFullDFTest method" should "finish a succeed execution" in {
    val df: DataFrame = config.readInputs().getGlobalFullDF
    df.show(30)
    succeed
  }
  //Prueba de falla cuando el  GetGlobalFullDF cuando las llaves no son iguales
  "GetGlobalFullDFTest method" should "throw a JoinException" in {
    assertThrows[JoinException] {
      val df: DataFrame = configJoinException.readInputs().getGlobalFullDF
      df.show
    }
  }
}
