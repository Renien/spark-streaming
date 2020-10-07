package util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.TimestampType
import com.uber.h3core.H3Core

/**
 * Utility object to that contains all ad-hoc user defined functions.
 */
object UdfUtil {

  /**
   * parsePartition parses event timestamp to get 5 minute partitions
   *
   * @return long
   */
  def parseTsUDF: UserDefinedFunction = udf((created_at: Long) => created_at / 1e-9)

  def tagCellUDF = udf((lat: Double, lng: Double) => H3Core.newInstance.geoToH3Address(lat, lng, 8))

}

