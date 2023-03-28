import org.apache.iotdb.session.pool.{SessionDataSetWrapper, SessionPool}
import utils.Utils.{getIoTDBSession, getLeafNode}

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util

object Query {

  def main(args: Array[String]): Unit = {

  // 结果输出到文件
  val path = "iotdb_test.log"
  val logFile = new File(path)
  val fileOutputStream = new FileOutputStream(logFile, true)
  val channel = fileOutputStream.getChannel


  //查询最后一条数据
  val paths = new util.ArrayList[String]()
  paths.add("root.test3.**")

  val sessionPool: SessionPool = getIoTDBSession.build()

  var start: Long = System.nanoTime()

  val dataSet1: SessionDataSetWrapper = sessionPool.executeLastDataQuery(paths)

  getLeafNode(dataSet1) foreach println

  val qureyCast = System.nanoTime() - start
  val qureyCast_log = s"写入数据耗时：${(qureyCast.toFloat / 1000000000).formatted("%.3f")}s\n"
//  channel.write(ByteBuffer.wrap(qureyCast_log.getBytes))
  println(qureyCast_log)

  }
}
