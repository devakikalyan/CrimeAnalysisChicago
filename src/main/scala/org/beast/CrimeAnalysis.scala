package org.beast
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.ESJPredicate
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.indexing.RSGrovePartitioner
import edu.ucr.cs.bdlab.davinci.MVTDataVisualizer
import edu.ucr.cs.bdlab.beast.util.BeastServer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import java.io.PrintWriter

object CrimeAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Crime Analysis with Beast")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = spark.sparkContext

    try {
      val crimesPath = "Chicago_Crimes.geojson"
      val zipCodesPath = "TIGER2018_ZCTA5.geojson"

      val crimesRDD: RDD[IFeature] = sparkContext.geojsonFile(crimesPath)
      val zipCodesRDD: RDD[IFeature] = sparkContext.geojsonFile(zipCodesPath)

      // Spatial join to find crimes within each ZIP code
      val crimesWithZipCodes = crimesRDD.spatialJoin(zipCodesRDD, ESJPredicate.Intersects)

      // Aggregate crimes by ZIP code
      val crimeCountsByZipCode = crimesWithZipCodes.map {
          case (_, zipCodeFeature) => (zipCodeFeature.getAs[String]("ZCTA5CE10"), 1)
        }
        .reduceByKey(_ + _)

      // Print the number of crimes per ZIP code
      crimeCountsByZipCode.collect().foreach {
        case (zipCode, count) => println(s"ZIP Code: $zipCode, Number of Crimes: $count")
      }

      // Join crime counts with ZIP code geometries
      val zipCodeGeometries = zipCodesRDD.map(feature => (feature.getAs[String]("ZCTA5CE10"), feature.getGeometry))
      val crimeCountsWithGeometry = crimeCountsByZipCode.join(zipCodeGeometries)

      // Generate GeoJSON features
      val geoJsonFeatures = crimeCountsWithGeometry.map { case (zipCode, (count, geometry)) =>
        val coordinates = geometry.getCentroid.getCoordinate
        s"""|{
            |  "type": "Feature",
            |  "properties": {
            |    "ZIP Code": "$zipCode",
            |    "Number of Crimes": $count
            |  },
            |  "geometry": {
            |    "type": "Point",
            |    "coordinates": [${coordinates.x}, ${coordinates.y}]
            |  }
            |}""".stripMargin
      }.collect()

      val geoJsonString = s"""|{
                              |  "type": "FeatureCollection",
                              |  "features": [
                              |    ${geoJsonFeatures.mkString(",\n")}
                              |  ]
                              |}""".stripMargin

      val writer = new PrintWriter("visualization.geojson")
      writer.write(geoJsonString)
      writer.close()
      println("GeoJSON file saved successfully.")

    } finally {
      spark.stop()
    }
  }
}
