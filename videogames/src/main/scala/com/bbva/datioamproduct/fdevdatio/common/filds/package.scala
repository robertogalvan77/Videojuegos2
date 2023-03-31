package com.bbva.datioamproduct.fdevdatio.common

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlphabeticsConfigConstans.{E, M, T}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.GenderConfigConstans._
package object filds {
  // Método para obtener columna y nombre de ventas de japon
  case object JaponSales extends Field{
    override val name: String = "jp_sales_per"
  }
  // Método para obtener columna y nombre de ventas de eu
  case object EuSales extends Field{
    override val name: String = "eu_sales_per"
  }
  // Método para obtener columna y nombre de CutooffDate
  case object CutoffDate extends Field{
    override  val name: String = "cutoff_date"
  }
  // Método para obtener columna y nombre de ventas globales
  case object GlobalSales extends Field {
    override val name: String = "global_sales_per"
  }
  // Método para obtener columna y nombre del promedio de ventas en japon
  case object AvgVentasJapon extends Field{
    override val name: String = "avg_jp_sales"

    def apply(): Column = avg(JaponSales.column) alias name
  }
  // Método para obtener columna y nombre del promedio de ventas en EU
  case  object AvgVentasEUA extends Field{
    override val name: String = "avg_eu_sales"

    def apply(): Column = avg(EuSales.column) alias name
  }
  // Método para obtener columna y nombre del promedio de ventas globales
  case object AvgVentasGlobal extends Field {
    override val name: String = "avg_global_sales"

    def apply(): Column = avg(GlobalSales.column) alias name
  }
  // Método para obtener columna y nombre del año
  case object ReleaseYear extends Field {
    override val name: String = "release_year"
  }
  // Método para obtener columna y nombre de la plataforma
  case object Platform extends Field {
    override val name: String = "platform_na"
  }
  // Método para obtener columna y nombre del id del videojuego
  case object VideogameID extends Field{
    override  val name: String = "videogame_id"
  }
  // Método para obtener columna y nombre del nombre del videojuego
  case object VideogameName extends Field {
    override val name: String = "videogame_name"
  }
  // Método para obtener la suma de las ventas globales
  case object CountGlobalSales extends Field {
    override val name: String = "count_global_sales"

    def apply(): Column = sum(GlobalSales.column) alias name
  }
  // Método para obtener columna y nombre del genero del videojuego
  case object VideoGameGenre extends Field {
    override val name: String = "videgoame_genre"
  }
  // Método para obtener columna y nombre del nombre publicado
  case object PublisherName extends Field {
    override val name: String = "publisher_name"
  }

  // Método para obtener columna y nombre de otras ventas por año
  case object OtherSalesPer extends Field {
    override val name: String = "other_sales_per"
  }
// Metodo para obtener el Na sales
  case object NaSalesPer extends Field {
    override val name: String = "na_sales_per"
  }


  // Método para obtener el nombre completo mediante la concatenación
  case object CompleteName extends Field {
    override val name: String = "complete_name"

    def apply(): Column = {
      concat(PublisherName.column, lit("_"), Platform.column) alias (name)
    }
  }
  // Método para obtener la clasificacion de los videojuegos
  case object Clasification extends Field {
    override val name: String = "clasification"

    def apply(): Column = {
      when(PublisherName.column === Nintendo, E)
        .when(VideoGameGenre.column  === Shoter, M)
        .when(VideoGameGenre.column  === Figtig, M)
        .when(VideoGameGenre.column  === RolePlaying, T)
        .otherwise(E) alias name
    }
  }



}
