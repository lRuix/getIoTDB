import org.apache.iotdb.session.pool.SessionPool
import org.apache.iotdb.session.{Session, SessionDataSet}
import org.apache.iotdb.tsfile.file.metadata.enums.{CompressionType, TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema
import utils.InsertData
import utils.Utils.getIoTDBSession

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object WriteTestData {

  def main(args: Array[String]): Unit = {

    // 结果输出到文件
    val path = "iotdb_test.log"
    val logFile = new File(path)
    val fileOutputStream = new FileOutputStream(logFile, true)
    val channel = fileOutputStream.getChannel

    val vinNum = 1000
    val sigNum = 100
    val rowSize = 10000
    val outOrderRate = 0.01
    val dataOverView =
      s"""数据总览：
         | vin数量：$vinNum
         | 单vin信号量：$sigNum
         | 单信号数据量：$rowSize
         | 乱序率：$outOrderRate
         | 总计数据点：${vinNum * sigNum * rowSize}
         |    """.stripMargin

    println(dataOverView)
    channel.write(ByteBuffer.wrap(dataOverView.getBytes))

    val paths = new ArrayBuffer[String]()
    val sigs = new ArrayBuffer[MeasurementSchema]()
    (0 until vinNum).foreach(num => paths.append(s"root.test3.tank.tank500.LL${num}"))
    (0 until sigNum).foreach(num => sigs.append(new MeasurementSchema(s"s$num", TSDataType.DOUBLE)))


    val sessionPool = getIoTDBSession.build()

    //    sessionPool.createDatabase("root.test3") //创建数据库

    // 创建时间序列（按设备创建）
    //    paths.foreach(path => {
    //      sigs.foreach(sig => sessionPool.createTimeseries(path + "." + sig.getMeasurementId, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY))
    //    })

    println("开始生成数据......")
    var start = System.nanoTime()

    val tablets = InsertData.createTablets(paths, sigs, rowSize, disOrderRate = outOrderRate) // 创建测试数据集

    val createCast = System.nanoTime() - start
    val createCast_log = s"生成数据耗时：${(createCast.toFloat / 1000000000).formatted("%.3f")}s\n"
    println(dataOverView)
    channel.write(ByteBuffer.wrap(createCast_log.getBytes))
    println(createCast_log)

    println("开始写入数据......")
    start = System.nanoTime()

    tablets.foreach(sessionPool.insertTablet) //写入数据

    val writeCast = System.nanoTime() - start
    val writeCast_log = s"写入数据耗时：${(writeCast.toFloat / 1000000000).formatted("%.3f")}s\n"
    channel.write(ByteBuffer.wrap(writeCast_log.getBytes))
    println(writeCast_log)



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
