package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.VideoGamesConfigConstants.InputTag
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

package object utils {

  implicit class VideoGamesConfig(config:Config) extends IOUtils{
    def readInputs():Map[String,DataFrame] = {
      config.getObject(InputTag).keySet()
        .map(key =>{
          val inputConfig:Config = config.getConfig(s"$InputTag.$key")
          (key,read(inputConfig))
        }
        ).toMap
    }
  }

}
