/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import twitter4j.json.DataObjectFactory
import org.geotools.data.shapefile.ShapefileDataStore
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.MultiPolygon

object USStateCounter {

  /**
   * This code shows how to count the number of geocoded tweets in each state
   * using OpenGIS and shape files
   *
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StateCounter")
    val sc = new SparkContext(conf)
    
    // Tweets to read
    val dataPath = args(0)
    
    // Shape file with shapes for the US states
    //  I used the shape file from the Matplotlib examples on github
    //  URL: https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp
    val shapePath = args(1)
    
    // Create a list of polygons that correspond to the US States
    //  We'll use these for testing whether a state contains a given GPS point
    val statePolyList = generateStatePolyList(shapePath)
    
    // Broadcast that polygon list for quick access on the cluster nodes
    val broad_StatePolyList = sc.broadcast(statePolyList)
    
    // Read the tweets we'll process
    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)
    
    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 2 ) {
      val initialPartitions = args(2).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size
    
    // Convert each JSON line in the file to a status using Twitter4j
    //  Note that not all lines are Status lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.'
    val tweets = twitterMsgs.map(line => {
        try {
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    
    // Only keep non-null status with text
    val tweetsFiltered = tweets.filter(status => {       
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })
    
    // Filter again to keep tweets with a geo feature
    val geoTweets = tweetsFiltered.filter(status => {
        status.getGeoLocation != null
      })
    
    // For each geocoded tweet, pull its lat/lon and test whether it is in
    //  a US state. If so, map to a tuple (state name, 1) for grouping and 
    //  counting
    val tweetsToStates : RDD[Tuple2[String, Int]] = geoTweets.map(status => {
        val geoLoc = status.getGeoLocation
        val lat = geoLoc.getLatitude
        val lon = geoLoc.getLongitude
        
        // Create a test point from the tweet's lat/lon
        val geometer = new GeometryFactory()
        val testCoordinate = new Coordinate(lon, lat)
        val testGeo = geometer.createPoint(testCoordinate)
        
        // Get the polygon list from the broadcast value
        val polyList = broad_StatePolyList.value
        
        var stateName : String = null
        
        // Index for searching the polygon list
        var index = 0
        
        // For each polygon and whle we have not found a state name, test if the
        //  test point is in the corresponding state's polygon
        while ( index < polyList.size && stateName == null ) {
          val polyTuple = polyList(index)
          val thisState = polyTuple._1
          val polygon = polyTuple._2
          
          if ( polygon.contains(testGeo) ) {
            stateName = thisState
          }
          
          // Advance the list index
          index = index + 1
        }

        // Special key for non-US tweets
        if ( stateName == null ) {
          stateName = "_OUTSIDE USA"
        }
        
        (stateName, 1)
      })
    
    // Group the state count tuples, so we can count them
    val stateGroups = tweetsToStates.groupByKey()
    
    // Map the grouped RDD to a tuple of state and count, then collect to driver
    //  and sort by state name
    val stateCounts : Array[Tuple2[String, Int]] = stateGroups.mapValues(l => l.size).collect.sorted
    
    // Print each state's counts
    for ( statePair <- stateCounts ) {
      println(statePair._1 + ",\t" + statePair._2)
    }
  }

  /**
   * Given the path to a shape file, generate a list of tuples containing state
   *  names and polygons. This list will likely have multiple polygons per state
   *
   * @param shapePath the path to the shape file to use
   */
  def generateStatePolyList(shapePath : String) : List[Tuple2[String,MultiPolygon]] = {

    var polyList : List[Tuple2[String,MultiPolygon]] = List.empty
    
    val file = new File(shapePath);
    val dataStore = new ShapefileDataStore(file.toURI().toURL())

    val reader = dataStore.getFeatureReader()
    while ( reader.hasNext ) {
      val ftr = reader.next
      val stateName : String = ftr.getAttribute("NAME").asInstanceOf[String]
      val polygon : MultiPolygon = ftr.getDefaultGeometry().asInstanceOf[MultiPolygon]

      polyList = polyList :+ (stateName, polygon)
    }
    
    return polyList
  }
}
