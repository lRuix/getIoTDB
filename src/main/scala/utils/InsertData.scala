package utils

import org.apache.iotdb.tsfile.write.record.Tablet

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object InsertData {

  import org.apache.iotdb.tsfile.write.schema.MeasurementSchema

  import java.util

  /**
   * return a set of timestamp
   *
   * @param initTime     start timestamp
   * @param num          number returned
   * @param step         The distance between two adjacent timestamps
   * @param disOrderRate 0-1.0 disOrderRate of the returned timestamps are out of order
   * @param range        If the two adjacent times are disordered, the interval between them is range
   * @return a set of timestamp
   */
  private def genTimestamps(initTime: Long = 1640966400000L, num: Int, step: Int, disOrderRate: Double, range: Int): mutable.Seq[Long] = {

    val times = new ArrayBuffer[Long]
    val disor = scala.util.Random.shuffle((0 until num).toList).toArray.slice(0, (num * disOrderRate).toInt).toList
    (0 until num).foreach(n => {

      val tmp = initTime + step * n
      if (disor.contains(n)) {
        times.append(tmp + scala.util.Random.nextInt(range))
      }
      times.append(tmp)
    })
    times
  }

  /**
   * return a set of Tablet
   *
   * @param paths        a set of devicePath: ["root.ln.LL1"]
   * @param sensors      a set of MeasurementSchema
   * @param rowSize      the number of data in a Tablet
   * @param initTime     start timestamp
   * @param step         The distance between two adjacent timestamps
   * @param disOrderRate 0-1.0 disOrderRate of the returned timestamps are out of order
   * @param range        If the two adjacent times are disordered, the interval between them is range
   * @return a set of Tablet
   */
  def createTablets(paths: Seq[String], sensors: Seq[MeasurementSchema], rowSize: Int, initTime: Long = 1640966400000L, step: Int = 1000, outOrderRate: Double = 0.01, range: Int = 100): Seq[Tablet] = {
    //    val deviceId = "root.test.tank.tank500.LL1"
    //    val initTime = 1640966400000L //20220101 00:00:00

    val schemas = new util.ArrayList[MeasurementSchema]

    val tablets = new ArrayBuffer[Tablet]()
    val timestamps = genTimestamps(initTime, rowSize, step, outOrderRate, range)
    sensors.foreach(schemas.add)

    paths.foreach(device => {
      val ta = new Tablet(device, schemas, rowSize)
      ta.rowSize = rowSize

      (0 until rowSize).foreach(row => {
        ta.addTimestamp(row, timestamps(row))
        sensors.foreach(sensor => {
          ta.addValue(sensor.getMeasurementId, row, scala.util.Random.nextGaussian())
        })
      })
      tablets.append(ta)
    })
    tablets
  }
}
