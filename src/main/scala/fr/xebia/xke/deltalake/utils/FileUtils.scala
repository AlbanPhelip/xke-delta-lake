package fr.xebia.xke.deltalake.utils

import java.io._

object FileUtils {

  def delete(file: String): Unit = {
    delete(new File(file))
  }

  def delete(file: File): Unit = {
    if(file.isDirectory){
      Option(file.listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(delete)
    }
    file.delete()
  }

}
