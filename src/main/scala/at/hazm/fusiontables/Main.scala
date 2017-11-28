package at.hazm.fusiontables

import java.io.FileInputStream
import java.util
import java.util.Date

import scala.collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.batch.json.JsonBatchCallback
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.api.client.http.HttpHeaders
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.drive.{Drive, DriveScopes}
import com.google.api.services.drive.model.Permission
import com.google.api.services.fusiontables.model.{Column, Sqlresponse, Table}
import com.google.api.services.fusiontables.{Fusiontables, FusiontablesScopes}

object Main {
  def main(args:Array[String]):Unit = {
    val secretFile = args(0) // MOXBOX-000000000000.json のようなファイル名

    // 認証情報の作成
    val in = new FileInputStream(secretFile)
    val credential = GoogleCredential.fromStream(in)
    in.close()
    println(s"Service Account ID: ${credential.getServiceAccountId}")
    println(s"Service Account User: ${credential.getServiceAccountUser}")
    println(s"Service Account Project ID: ${credential.getServiceAccountProjectId}")
    println(s"Service Method: ${credential.getMethod}")

    val fusiontables = new Fusiontables.Builder(
      new NetHttpTransport(), new JacksonFactory(),
      credential.createScoped(Seq(FusiontablesScopes.FUSIONTABLES).asJava)
    ).setApplicationName("MOXBOX-Sample").build()

    println(Option(fusiontables.table().list().execute().getItems).map(_.asScala).getOrElse(List.empty).mkString("[", ",", "]"))

    val tableId = "1o9p9MpIDzGFndhp6DN76b_Ve2PiiCuqXzFkvT06G"
    val batch = fusiontables.batch()
    val callback = new JsonBatchCallback[Sqlresponse] {
      override def onFailure(e:GoogleJsonError, responseHeaders:HttpHeaders):Unit = println(s"Failure: $e")

      override def onSuccess(t:Sqlresponse, responseHeaders:HttpHeaders):Unit = {
        t.get()
        println(s"Success: $t")
      }
    }
    case class User(id:String, name:String, location:(Double, Double), register:Date = new Date())
    Seq(
      User("82502402-6cf4-4da2-a2f5-5d751f6d08df", "北海道 太郎", (43.064308, 141.346833)),
      User("f41ba42b-de83-4aef-a7d8-e4b7489f39eb", "仙台 次郎", (38.268197, 140.869416)),
      User("d7e9c328-1ce8-4462-bb7b-6c3b9615ee30", "東京 三郎", (35.689620, 139.692106)),
      User("8b5f16b4-6c1f-4286-88a8-662add7b907f", "名古屋 四郎", (35.180236, 136.906689)),
      User("8f8c655e-02c4-4849-8975-593a1d807240", "大阪 五郎", (34.693736, 135.502206)),
      User("0e78b8c4-233e-4a2d-b94f-8ba296169c47", "博多 六郎", (33.590119, 130.401716))
    ).foreach { user =>
      fusiontables.query().sql(s"INSERT INTO $tableId(ID,USER_NAME,LOCATION,REGISTER) VALUES('${user.id}','${user.name}','<Point><coordinates>${user.location._2},${user.location._1}</coordinates></Point>','${user.register}')").queue(batch, callback)
    }
    batch.execute()

    // 行の取得
    val res = fusiontables.query().sqlGet(s"SELECT ROWID,ID,USER_NAME,LOCATION,REGISTER FROM $tableId WHERE USER_NAME STARTS WITH '大阪'").execute()
    println(res)
    val rowId = res.getRows.get(0).get(0)
    println(s"ROWID=$rowId")

    // 行の更新
    println(fusiontables.query().sql(s"UPDATE $tableId SET USER_NAME='大阪 万次郎' WHERE ROWID='$rowId'").execute())

    // 行の削除
    println(fusiontables.query().sql(s"DELETE FROM $tableId WHERE ROWID='$rowId'").execute())

    // テーブルの作成
    val newTableId = locally {
      val key = new Column()
      key.setName("KEY")
      key.setType("STRING")

      val value = new Column()
      value.setName("VALUE")
      value.setType("STRING")

      val newTable = new Table()
      newTable.setColumns(util.Arrays.asList(key, value))
      newTable.setName("MOXBOX NEW TABLE")
      newTable.setIsExportable(true)
      val result = fusiontables.table().insert(newTable).execute()
      println(result)
      result.getTableId
    }

    // パーミッションの追加
    locally {
      val permission = new Permission()
      permission.setEmailAddress("xxxx@gmail.com")
      permission.setType("user")
      permission.setRole("writer")
      val drive = new Drive.Builder(
        new NetHttpTransport(), new JacksonFactory(),
        credential.createScoped(util.Arrays.asList(DriveScopes.DRIVE))
      ).setApplicationName("MOXBOX-Sample").build()
      drive.files().list().execute().getFiles.asScala.foreach { file =>
        println(file)
      }
      println(drive.permissions().create(newTableId, permission).execute())
    }

    // テーブルの削除
    locally {
      println(fusiontables.table().delete(newTableId).execute())
    }
  }
}