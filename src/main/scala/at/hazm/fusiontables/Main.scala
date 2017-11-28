package at.hazm.fusiontables

import java.io.FileInputStream

import scala.collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.fusiontables.model.{Column, Table}
import com.google.api.services.fusiontables.{Fusiontables, FusiontablesScopes}

object Main {
  def main(args:Array[String]):Unit = {
    val privateKeyFile = args(0)   // MOXBOX-000000000000.json のようなファイル名
    val in = new FileInputStream(privateKeyFile)
    val credential = GoogleCredential
      .fromStream(in)
      .createScoped(Seq(FusiontablesScopes.FUSIONTABLES).asJava)
    in.close()

    val fusiontables = new Fusiontables.Builder(
      new NetHttpTransport(), new JacksonFactory(), credential)
        .setApplicationName("MOXBOX-Sample").build()
    println(Option(fusiontables.table().list().execute().getItems).map(_.asScala).getOrElse(List.empty).mkString("[", ",", "]"))

    val newTable = new Table()
    newTable.setColumns(Seq(
      new Column()
    ).asJava)
    fusiontables.table().insert(newTable)
  }
}