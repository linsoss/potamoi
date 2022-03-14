package com.github.potamois.potamoi.commons

/**
 * @author Al-assad
 */
object StringImplicits {

  implicit class StringWrapper(str: String) {

    // remove "\n" from string
    def compact: String = str.split("\n").map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

}
