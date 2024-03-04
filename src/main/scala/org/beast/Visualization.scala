import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import edu.ucr.cs.bdlab.davinci.MVTDataVisualizer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import edu.ucr.cs.bdlab.beast.util.BeastServer
import edu.ucr.cs.bdlab.beast.common.BeastOptions

object Visualization {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Generate Vector Tiles")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    val geojsonPath = "visualization.geojson"
    val features = sc.geojsonFile(geojsonPath)
    val opts = Map("threshold" -> "0")
    val tiles = MVTDataVisualizer.plotAllTiles(features, levels = 0 to 11, resolution = 256, buffer = 5, opts)
    val outputZipPath = "provinces_mvt.zip"
    MVTDataVisualizer.saveTilesCompact(tiles, outputZipPath, opts)
    println("Vector tiles generated and saved to $outputZipPath")
    new BeastServer().run(new BeastOptions(), null, null, sc)

  }
}
