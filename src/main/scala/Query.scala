import org.apache.iotdb.session.pool.{SessionDataSetWrapper, SessionPool}
import utils.Utils.{getIoTDBSession, getLeafNode}

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.collection.mutable.ArrayBuffer

object Query {

  def looper(sessionPool: SessionPool, num: Int, sql: String, channel: FileChannel): Int = {

    val timeSeq = new ArrayBuffer[Float]
    var start = System.nanoTime()
    val cast = (x: Long) => ((System.nanoTime() - x).toFloat / 1000000000).formatted("%.3f").toFloat

    println("开始执行：" + sql)
    println(s"共计 $num 轮")

    1 to num foreach (n => {
      start = System.nanoTime()

      val wrapper = sessionPool.executeQueryStatement(sql)

      getLeafNode(wrapper)

      val diffTime = cast(start)
      println(s"第 $n 轮，查询耗时：${diffTime}s")
      timeSeq.append(diffTime)
    })

    val avgCast = (timeSeq.sum / timeSeq.size).formatted("%.3f").toFloat
    val varCast = timeSeq.reduce(avgCast - _ + avgCast - _) / timeSeq.size


    println(s"执行结束：$sql")
    println(s"平均耗时：${avgCast}s")
    channel.write(ByteBuffer.wrap("\n".getBytes))
    channel.write(ByteBuffer.wrap(s"${"=" * 100}\n".getBytes))
    channel.write(ByteBuffer.wrap(s"执行语句：$sql\n".getBytes))
    channel.write(ByteBuffer.wrap(s"总计 $num 轮，平均耗时：${avgCast}s\n".getBytes))
    channel.write(ByteBuffer.wrap(s"总计 $num 轮，耗时波动：${varCast}s\n".getBytes))
    channel.write(ByteBuffer.wrap(s"${"=" * 100}\n".getBytes))
    channel.write(ByteBuffer.wrap("\n".getBytes))
  }

  def main(args: Array[String]): Unit = {

    // 结果输出到文件
    val path = "iotdb_test.log"
    val logFile = new File(path)
    val fileOutputStream = new FileOutputStream(logFile, true)
    val channel = fileOutputStream.getChannel

    val dataBases = "root.test3"
    val sql1 = s"select last * from $dataBases.**" //全局最新数据点查询
    val sql2 = s"select last s56 from $dataBases.**" //所有设备s56传感器最新数据点查询
    val sql3 = s"select last * from $dataBases.** where time < 2023-03-28 10:00:00" //特定时间全局最新数据点查询
    val sql4 = s"select last s56 from $dataBases.** where time < 2023-03-28 10:00:00" //特定时间所有设备s56传感器最新数据点查询
    val sql5 = s"select s56 from $dataBases.**.LL70 where s56 between -1 and 1" //过滤条件查询
    val sql6 = s"select count(s70), max_value(s72) from $dataBases.**.LL70 group by ([2023-03-28 00:00:00, 2023-03-28 10:00:00), 30m, 28m)" //滑动窗口
    val sql7 = s"select count(s23) from root.** group by level = 1" //普通聚合
    val sql8 = s"select M4(s1,'timeInterval'='25') from $dataBases.**.LL70" //数据降采样
    val sql9 = s"select acf(s56) from $dataBases.**.LL338" //序列自相关计算
    val sql10 = s"select integral(s1) from $dataBases.**.LL338" //数值积分计算
    val sql11 = s"select minmax(s78) from $dataBases.**.LL338" //min-max标准化计算
    val sqls = Seq(sql1,sql2,sql3,sql4,sql5,sql6,sql7,sql8,sql9,sql10,sql11)

    val sessionPool: SessionPool = getIoTDBSession("application.properties",5).build()

    sqls foreach (sql=>looper(sessionPool, 100, sql, channel))


  }

}
