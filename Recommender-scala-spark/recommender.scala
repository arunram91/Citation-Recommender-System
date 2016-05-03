/** Imports */
/** Recommender System using spark's mllib
Authors: Ganesh Nagarajan, Neha Bisht
 */
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.io.Source
import java.io._

object Recommender {
  def start(): Unit = {

    /** Configuration Parameters */
    val rank = 20
    val numIterations = 30
    val graphFile = "/home/ganesh/Desktop/graph.csv"
    val similarityFile = "/home/ganesh/spark/bin/similarity_result.txt"
    val lambda = 0.01
    val alpha = 0.01

    /** File Handlers */
    val docFile = sc.textFile(graphFile)
    val docSimilarity = Source.fromFile(similarityFile).getLines()

    /**
     * Read the graph file to create input items, citing paper and the
     * cited paper for the matrix factorization. Here the Citing paper
     * is considered as user and the Cited Paper is considered as the product.
     * We are trying to recommend the products to the user
     */

    val kvpair = docFile.map { line =>
      val content = line.split(",")
      (content(4), content(0))
    }
    val uniqueCitingIDS = kvpair.map(x => x._1).distinct
    val uniqueCitedIDS = kvpair.map(x => x._2).distinct
    val CitingLookup = uniqueCitingIDS.zipWithIndex
    val CitedLookup = uniqueCitedIDS.zipWithIndex
    val CitingLookups = sc.broadcast(CitingLookup.map(x => x).collectAsMap)
    val CitedLookups = sc.broadcast(CitedLookup.map(x => x).collectAsMap)
    val CitingReverseLookup = sc.broadcast(CitingLookup.map(x => (x._2, x._1)).collectAsMap)
    val CitedReverseLookup = sc.broadcast(CitedLookup.map(x => (x._2, x._1)).collectAsMap)

    /**
     * Create Ratings class for the matrix factorization Model.
     * Since this is an implicit rating, a binary rating is chosen.
     */
    val ratings = kvpair.map(x => Rating(CitingLookups.value(x._1).toInt,
      CitedLookups.value(x._2).toInt, 1.toDouble))

    /** Train an implicit recommendation model */
    //val model = findBestModel(ratings)
    val model = ALS.trainImplicit(ratings, 45, 15, 0.1, 95)


    /** Load the Similarity File */

    val similarityDS = docSimilarity.map { line =>
      val item = line.replace("[", "").replace("]", "").split(",")
      (item(0), Array(item(1), item(2), item(3), item(4), item(5)))
    }

    val broadcastedlookup = docFile.map { line =>
      val content = line.split(",")
      (content(9), content(10))
    }.collectAsMap


    val example=similarityDS.flatMap(item=>
    item._2.map{ids =>
    val id=CitingLookups.value(ids);
    val recommendations = model.recommendProducts(id.toInt, 20);
    recommendations.map(x => item._1+":"+ids+":"+CitedReverseLookup.value(x.product)+":"+x.rating+"\n")})
    val write=example.map(x=> x.mkString(""))
    val fileop=new File("/home/ganesh/Desktop/output.csv")
    val bw=new BufferedWriter(new FileWriter(fileop))
    write.foreach(bw.write)
    bw.close()

  }
  def findBestModel(data : RDD[Rating]): MatrixFactorizationModel={
    //val rank=Range(5,50,5).toList
    val rank=Array(45)
    val lambda=Range.Double(0.001,0.01,0.001).toList
    //val lambda=Array(0.2)
    //val alpha=Range.Double(50,100,5).toList
    val alpha=Array(95)
    val BestRMSEVal=100.0
    val BestModel: MatrixFactorizationModel = ALS.trainImplicit(data, 10 , 20, 0.01, 0.01)

    val strippedrating=data.map{case Rating(user,product,rating) => (user,product)}
    val bestParameters=""

    for (r <-rank){
      for (l <- lambda){
        for (a <-alpha){
          println(" "+r+" "+l+" "+a)
          val model = ALS.trainImplicit(data, r, 15, l, a)
          val predicted = model.predict(strippedrating).map(x => x.rating)
          val mse=predicted.map{x=> val error=1-x; error*error }.mean()
          println(mse)
          if (mse < BestRMSEVal){
            val BestModel=model
            val bestParameters=" "+r+" "+l+" "+a
          }
        }
      }
    }
    print(("The Best MSE"),BestRMSEVal)
    return(BestModel)
  }
}
Recommender.start()
System.exit(0)
