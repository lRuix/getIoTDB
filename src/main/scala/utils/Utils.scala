package utils

import org.apache.iotdb.session.SessionDataSet
import org.apache.iotdb.session.pool.{SessionDataSetWrapper, SessionPool}

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object Utils {

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

  def getLeafNode(se: SessionDataSetWrapper): Seq[String] = {
    val tmp = new ArrayBuffer[String]()
    while (
      se.hasNext
    ) {
      tmp.append(se.next.getFields.get(0).toString)
    }
    tmp
  }

}
