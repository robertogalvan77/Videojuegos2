package com.bbva.datioamproduct.fdevdatio.common

object StaticVals {

  case object VideoGamesConfigConstants {
    val RootTag: String = "videoGamesJob"
    val InputTag: String = s"$RootTag.input"
    val FdevGamesInfoTag: String = s"fdevVideoGamesInfo"
    val FdevGamesSales: String = s"fdevVideoGamesSales"
  }

  case object NumericConfigConstans {
    val Zero: Int = 0

  }

    case object AlphabeticsConfigConstans{
    val E: String = s"E"
    val M: String = s"M"
    val T: String = s"T"
  }

  case object GenderConfigConstans {
    val Nintendo: String = s"Nintendo"
    val Shoter: String = s"Shooter"
    val Figtig: String = s"Fighting"
    val RolePlaying: String = s"Role-Playing"
  }
  case object TypeJoin{
    val LeftJoin: String = s"left"
    val LeftAnti: String = s"leftanti"
  }

  case object ListFilter {
    val listXbox: List[String] = List("XB", "XONE", "X360")
    val arrXbox: Array[Any] = listXbox.toArray
    val listNintendo: List[String] = List("3DS", "GB", "GBA", "NES", "N64", "SNES", "Wii", "Wiiu")
    val arrNintendo: Array[Any] = listNintendo.toArray
    val listPlay: List[String] = List(" PS", "PS2", "PS3", "PS4", "PSP", "PSV")
    val arrPlay: Array[Any] = listPlay.toArray
  }
}
