import org.apache.iotdb.session.pool.SessionPool
import org.apache.iotdb.session.{Session, SessionDataSet}
import org.apache.iotdb.tsfile.file.metadata.enums.{CompressionType, TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema
import utils.InsertData
import utils.Utils.{getIoTDBSession, getLeafNode}

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object WriteTestData {

  def main(args: Array[String]): Unit = {

    // 结果输出到文件
    val prop = "application.properties"
    val path = "iotdb_test.log"
    val logFile = new File(path)
    val fileOutputStream = new FileOutputStream(logFile, true)
    val channel = fileOutputStream.getChannel
    val splitLine = (s: String) => "=" * 40 + s + "=" * 40
    val model = "root.test3.tank.tank500"

    val vinNum = 10
    val sigNum = 10
    val rowSize = 1000000

    val paths = new ArrayBuffer[String]()
    val sigs = new ArrayBuffer[MeasurementSchema]()
    (0 until vinNum).foreach(num => paths.append(s"$model.LL${num}"))
    (0 until sigNum).foreach(num => sigs.append(new MeasurementSchema(s"s$num", TSDataType.DOUBLE)))

    val sessionPool = getIoTDBSession(prop,5).build()

    sessionPool.createDatabase("root.test3") //创建数据库

    //     创建时间序列（按设备创建）
    paths.foreach(path => {
      sigs.foreach(sig => sessionPool.createTimeseries(path + "." + sig.getMeasurementId, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY))
    })

    def saveToIoT(outOrderRate: Seq[Double]) = {
      var i = 1
      val cast = (x: Long) => ((System.nanoTime() - x).toFloat / 1000000000).formatted("%.3f").toDouble
      val casts = new ArrayBuffer[Double]()
      var start: Long = 0L
      var initTime = 1679932800000L

      outOrderRate foreach (rate => {

        println(s"开始生成第 $i 轮数据......")
        start = System.nanoTime()
        // 创建测试数据集
        val tablets = InsertData.createTablets(paths, sigs, rowSize, initTime = initTime, outOrderRate = rate)
        val createCast_log = s"生成数据耗时：${cast(start)}s\n"
        println(createCast_log)
        println(s"开始写入第 $i 轮数据......乱序率：$rate")
        start = System.nanoTime()
        tablets.foreach(sessionPool.insertTablet) //写入数据
        val writeCast_log = s"第 $i 轮数据，乱序率 $rate 写入耗时：${cast(start)}s\n"
        casts.append(cast(start))
        println(writeCast_log)
        channel.write(ByteBuffer.wrap(writeCast_log.getBytes))

        i = i + 1
        initTime = sessionPool.executeQueryStatement("select last s9 from root.test3.tank.tank500.LL9").next.getTimestamp
      })

      val avgCast = casts.sum / casts.size
      val varCast = casts.reduce(avgCast - _ + avgCast - _) / casts.size
      val total_log = s"总计${outOrderRate.size}轮，总共耗时：${casts.sum.formatted("%.3f")}s，平均每轮耗时：${avgCast.formatted("%.3f")}s，写入波动：${varCast.formatted("%.3f")}s\n"
      channel.write(ByteBuffer.wrap((splitLine("写入摘要") + "\n").getBytes))
      channel.write(ByteBuffer.wrap("\n".getBytes))
      channel.write(ByteBuffer.wrap(total_log.getBytes))
    }

    channel.write(ByteBuffer.wrap((splitLine("======")+"\n").getBytes))
    channel.write(ByteBuffer.wrap((" " * 40 + "写入报告" + "\n").getBytes))
    channel.write(ByteBuffer.wrap((splitLine("======")+ "\n").getBytes))

    val rates = Seq(0.01, 0.1, 0.4, 0.9)
    saveToIoT(rates)
    channel.write(ByteBuffer.wrap("\n".getBytes))
    channel.write(ByteBuffer.wrap(splitLine("数据总览").getBytes))
    channel.write(ByteBuffer.wrap("\n".getBytes))

    val totalVin = getLeafNode(sessionPool.executeQueryStatement(s"COUNT NODES $model.** LEVEL=4")).head.toInt
    val totalSig = getLeafNode(sessionPool.executeQueryStatement(s"COUNT NODES $model.LL56.** LEVEL=5")).head.toInt
    val totalData:Long = totalSig.toLong * totalVin * rowSize * rates.size
    channel.write(ByteBuffer.wrap("\n".getBytes))
    channel.write(ByteBuffer.wrap(s"总的车辆数：$totalVin\n".getBytes))
    channel.write(ByteBuffer.wrap(s"单车辆信号数：$totalSig\n".getBytes))
    channel.write(ByteBuffer.wrap(s"总的数据量：$totalData\n".getBytes))
    channel.write(ByteBuffer.wrap("\n".getBytes))
















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


  }


}
