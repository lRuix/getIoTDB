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
    val sql1 = s"select last * from $dataBases.**"
    val sql2 = s"select last s56 from $dataBases.**"
    val sql3 = s"select last * from $dataBases.** where time < 2022-01-01 02:00:00"
    val sql4 = s"select last s56 from $dataBases.** where time < 2022-01-01 02:00:00"

    //查询最后一条数据
    //    val paths = new util.ArrayList[String]()
    //    paths.add(dataBases)

    val sessionPool: SessionPool = getIoTDBSession.build()
    //    looper(sessionPool,100,sql1,channel)
    looper(sessionPool, 100, sql3, channel)


  }

}
