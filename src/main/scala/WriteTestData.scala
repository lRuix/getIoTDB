import org.apache.iotdb.session.pool.SessionPool
import org.apache.iotdb.session.pool.{SessionDataSetWrapper, SessionPool}
import org.apache.iotdb.session.{Session, SessionDataSet}
import org.apache.iotdb.tsfile.file.metadata.enums.{CompressionType, TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.write.record.Tablet
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema
import sun.audio.AudioDevice

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object selectIoTDB {

  def getProperties(file: String, cols: String*): Array[String] = {

    val path = this.getClass.getClassLoader.getResource(file)

    val properties = new Properties()
    properties.load(path.openStream())
    val colsIter = cols.toIterator
    val props = new ArrayBuffer[String]()
    while (colsIter.hasNext) {
      props.append(properties.getProperty(colsIter.next()))
    }
    props.toArray
  }

  def getIoTDBSession: SessionPool.Builder = {
    val props = getProperties("application.properties", "host", "port", "user", "pwd")
    new SessionPool.Builder().host(props(0)).port(props(1).toInt).user(props(2)).password(props(3))
  }

  def getLeafNode(se: SessionDataSet): Seq[String] = {
    val tmp = new ArrayBuffer[String]()
    while (
      se.hasNext
    ) {
      tmp.append(se.next.getFields.get(0).toString)
    }
    tmp
  }

  def main(args: Array[String]): Unit = {

    // 结果输出到文件
    val path = "iotdb_test.log"
    val logFile = new File(path)
    val fileOutputStream = new FileOutputStream(logFile, true)
    val channel = fileOutputStream.getChannel

    val vinNum = 1000
    val sigNum = 100
    val paths = new ArrayBuffer[String]()
    val sigs = new ArrayBuffer[MeasurementSchema]()
    (0 until vinNum).foreach(num => paths.append(s"root.test3.tank.tank500.LL${num}"))
    (0 until sigNum).foreach(num => sigs.append(new MeasurementSchema(s"s$num", TSDataType.DOUBLE)))


    val sessionPool = getIoTDBSession.build()

    sessionPool.createDatabase("root.test3")

    paths.foreach(path => {
      sigs.foreach(sig => sessionPool.createTimeseries(path + "." + sig.getMeasurementId, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY))
    })

    var start = System.nanoTime()

    val tablets = insertData.genTablet(paths, sigs, 10000) // 创建数据集

    val createCast = System.nanoTime() - start
    val createCast_log = s"create data casts ${(createCast.toFloat / 1000000000).formatted("%.3f")}s"
    channel.write(ByteBuffer.wrap(createCast_log.getBytes))
    println(createCast_log)

    start = System.nanoTime()

    tablets.foreach(sessionPool.insertTablet) //写入数据

    val writeCast = System.nanoTime() - start
    val writeCast_log = s"write data casts ${(writeCast.toFloat / 1000000000).formatted("%.3f")}s"
    channel.write(ByteBuffer.wrap(writeCast_log.getBytes))
    println(writeCast_log)

    val a:Int = 3
    //    tablets.foreach(sessionPool.insertTablet(_))


    //    session.executeQueryStatement("delete from root.lrx2.tanke.tank500.LL14534149684.BMS_vol")
    //    session.executeQueryStatement("delete from root.lrx2.tanke.tank500.LL73485912382.BMS_temp")
    //    session.deleteTimeseries("root.lrx2.tanke.tank500.LL14534149684.BMS_vol")
    //    session.deleteTimeseries("root.lrx2.tanke.tank500.LL73485912382.BMS_temp")

    //    def createDatabase(paths: Seq[String]): Unit = {
    //      val rootPath = "create database root.test1."
    //      paths.foreach(r => {
    //        val createSQL = rootPath + r
    //        session.executeNonQueryStatement(createSQL)
    //        println(createSQL)
    //      })
    //    }

    //    def createDevice(nodes: Seq[String], deviceNum: Int, signalNum: Int) = {
    //
    //      val path = nodes.mkString(".")
    //      val paths = new ArrayBuffer[String]()
    //
    //      (1 to deviceNum).foreach(r => {
    //        val vin = path + "." + "l" + r.toString
    //
    //        (1 to signalNum).foreach(r => {
    //
    //          paths.append(vin + "." + "s" + r.toString)
    //
    //        })
    //      })
    //      paths
    //    }

    //    val devices = createDevice(Seq("tank", "tank500"), 500, 100)

    //    createDatabase(devices)

    //    println(getLeafNode(session.executeQueryStatement("show databases")).size)

    //    createDatabase(Seq("tank","tank300","l1","s1"))


    //    val dataSet = session.executeQueryStatement("show timeseries root.cc.坦克.坦克坦克300.CC2030BE21H.CC2030BE21H.汽油.基础款.LGWFF7A53NJ034146.*")

    //查询车辆数
    //    val dataSet = session.executeQueryStatement("count devices root.cc.**")
    // 车辆数：21877

    // 查询 品牌
    //    val dataSet = session.executeQueryStatement("show child nodes root.cc")
    //    WEY
    //    哈弗
    //    坦克
    //    欧拉

    // 查询 型号
    //    val dataSet = session.executeQueryStatement("show child nodes root.cc.*")
    //    WEYV71PHEV
    //    WEY拿铁
    //    WEY拿铁DHTPHEV
    //    WEY玛奇朵
    //    哈弗H6S
    //    哈弗神兽
    //    哈弗第三代H6
    //    坦克坦克300
    //    坦克坦克500
    //    欧拉芭蕾猫
    //    欧拉闪电猫
    // 查询设备
    //    val sql2 = "show devices root.cc.** limit 100"

    //    val sql3 = "select count(*) from root.cc.*.LGWFF7A5XNJ030661.** group by level=9"
    //    val sql3 = "show devices root.cc.**"
    //    val sql4 = "select count(*) from root.cc.*.*.*.*.*.*.LGWFF7A5XNJ030661 group by level=8"

    //    var dataSet:SessionDataSet = null
    //    var sql:String = null
    //    var datas:Int=0
    //    dataSet = session.executeQueryStatement("select count(*) from root.cc.坦克.坦克坦克300.CC2030BE21H.CC2030BE21H.汽油.基础款.LGWFF7A53NJ034146.** group by level=8")

    //    val vinList: Seq[String] = getLeafNode(dataSet)
    //    vinList.foreach(vin=>{
    //
    //      sql = s"select count(*) from ${vin}.** group by level=8"
    //      val size = session.executeQueryStatement(sql).next.getFields.get(0).toString.toInt
    //      datas + size
    //    })


    //        println(dataSet.next.getFields.get(0).toString)

    //    var sql:String = null
    //    var dataSet2: SessionDataSet = null

    //    getLeafNode(dataSet).foreach(deviceID=>{
    //      sql = s"select count(*) from " + deviceID + " group by level = 1"
    //      dataSet2 = session.executeQueryStatement(sql)
    //      getLeafNode(dataSet2).foreach(r=>{
    //        println(deviceID)
    //        println(r)
    //      })
    //    })


    //    list.foreach(println(_))
    //    println(list.size)

    sessionPool.close()
    //查询最后一条数据
    //    val paths = new util.ArrayList[String]()
    //    paths.add("root.lrx2.哈弗.哈弗酷狗.A08.CC6450BZ00A.fuel.`2021`.black.LGWEF5A53NK126240.veh_spd")
    //    val dataSet1 = session.executeLastDataQuery(paths)
  }


}
