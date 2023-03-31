package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.ListFilter._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.VideoGamesConfigConstants.{FdevGamesInfoTag, FdevGamesSales}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.TypeJoin.{LeftAnti, LeftJoin}
import org.apache.spark.sql.{Column, DataFrame}
import com.bbva.datioamproduct.fdevdatio.common.filds._
import org.apache.spark.sql.functions.{col, desc, row_number, year}
import org.apache.spark.sql.expressions.{Window}


package object transformations {
  // Error de JoinException cuando no se encuentran las llaves
  case class JoinException(expectedKeys: Array[String],
                           columns: Array[String],
                           location: String = "com.bbva.datioamproduct.fdevdatio.transformations.getFullDF",
                           message: String) extends Exception(message)

  implicit class DfTransformations(df: DataFrame) {
    // Método para el promedio de ventas de japon,eu y globales
    def avgSales: DataFrame = {
      df.agg(AvgVentasEUA(),AvgVentasJapon(),AvgVentasGlobal())

    }
    // Método para obtener el mínimo de las ventas globales
    def platformSales: DataFrame = {
      df.groupBy(Platform.column)
        .agg(CountGlobalSales())
        .orderBy(CountGlobalSales.column)
        .na.fill(0)
    }
    // Método para obtener el top 3 de los juegos vendidos (globales) por cada año
    def topVideoGames: DataFrame = {
      // Definimos las ventas por año y ventas globales
      val w = Window.partitionBy(year(ReleaseYear.column)).orderBy(desc(GlobalSales.name))
      // Agregar una nueva columna con el rank
      val rankedDF = df.addColumn(row_number.over(w).as("rank"))
      // Seleccionar las filas con rank menor o igual a 3
      rankedDF.filter("rank <= 3").drop("rank", "total_sales").sort(desc(ReleaseYear.name))

    }
    // Método para obtener el top 10 de ventas globales de xbox
    def xboxMaxSell: DataFrame = {
      df.filter(Platform.column.isin(arrXbox: _*))
        .sort(desc(GlobalSales.name))
        .limit(10)
      //        .select(VideoGameName.column, GlobalSalePer.column)
    }
    // Método para obtener el top 10 de ventas globales de nintendo
    def nintendoMaxSell: DataFrame = {
      df.filter(Platform.column.isin(arrNintendo: _*))
        .sort(desc(GlobalSales.name))
        .limit(10)
      //        .select(VideoGameName.column, GlobalSalePer.column)
    }
    // Método para obtener el top 10 de ventas globales de playStation
    def playMaxSell: DataFrame = {
      df.filter(Platform.column.isin(arrPlay: _*))
        .sort(desc(GlobalSales.name))
        .limit(10)
      //        .select(VideoGameName.column, GlobalSalePer.column)
    }
    // Método para agregar columna
    def addColumn(newColum: Column): DataFrame = {
      try {
        val colums: Array[Column] = df.columns.map(col) :+ newColum
        df.select(colums: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }
    // Método que une el DF de clasificacion con el de los top's de videojuegos por consola
    def getFinal(dfClasi: DataFrame, globalDatasF: DataFrame): DataFrame = {
      dfClasi.join(globalDatasF, Seq(VideogameID.name, VideogameName.name, Platform.name, ReleaseYear.name, VideoGameGenre.name, PublisherName.name), LeftJoin)
    }

  }


  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]) {
    @throws[JoinException]
    // Método que une el DF de informacion de videojuegos con el DF de ventas de videojuegos
    def getFullDF: DataFrame = {
      val dfGamesInfo: DataFrame = dfMap(FdevGamesInfoTag).drop(CutoffDate.column)
      val dfGamesSales: DataFrame = dfMap(FdevGamesSales).drop(CutoffDate.column)

      val GamesInfoKeys: Set[String] = Set(VideogameID.name)
      val GamesSalesKeys: Set[String] = Set(VideogameID.name)

      if(!GamesInfoKeys.subsetOf(dfMap(FdevGamesInfoTag).columns.toSet)) {
        throw JoinException(GamesInfoKeys.toArray,
          dfMap(FdevGamesInfoTag).columns,
          message = "Las llaves de players no coinciden con ninguna columna")
      }else if(!GamesSalesKeys.subsetOf(dfMap(FdevGamesSales).columns.toSet)) {
        throw JoinException(GamesSalesKeys.toArray,
          dfMap(FdevGamesSales).columns,
          message = "Las llaves de players no coinciden con ninguna columna")
      }
      else {
        dfGamesInfo
        .join(dfGamesSales, Seq(VideogameID.name), LeftJoin)
      }
    }
    // Método que genera el DF de los top's de videojuegos por consola
    def getGlobalFullDF: DataFrame = {
      val globalDF = dfMap
      val xboxExm: DataFrame = globalDF.getFullDF.xboxMaxSell
      val nintendoExm: DataFrame = globalDF.getFullDF.nintendoMaxSell
      val playExm: DataFrame = globalDF.getFullDF.playMaxSell
      val combinedDF = xboxExm.unionAll(nintendoExm).unionAll(playExm)
      combinedDF
    }
  }
}





