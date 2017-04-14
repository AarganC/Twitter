package dsti

/**
 * Aargan ${Aargan.Cointepas}
 */

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aargan-App-1")
    //val conf = new SparkConf().setMaster("local[*]").setAppName("Aargan-App-1")
    System.out.println("1")
    val sc = new SparkContext(conf)
    System.out.println("2")

    //Création d'un DStream, enregistrement de 30sec
    val ssc = new StreamingContext(sc, Duration(30000))
    //Clès et identifiant twitter
    System.setProperty("twitter4j.oauth.consumerKey", "OQoLagwCvHLxMs9j994GHyr7B")
    System.setProperty("twitter4j.oauth.consumerSecret", "pVqJwsxb3HyeFatTeteiNrtscpf9B3d05wpbxDWojWhPj3nYMh")
    System.setProperty("twitter4j.oauth.accessToken", "841687526259732480-OGpHgHDB4lu5TmhZmcz1C6ttFKfZhMQ")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "YSRM2bQeYtxXzTVHshgI2FJQvBKxtjoPNHGCyWK5VEZ4t")

    //Création d'un RDD
    val stream = TwitterUtils.createStream(ssc, None)
    System.out.println("3")

    <!--Recherche des twitt en Français-->
    //Filtrage par langue "fr" pour Français.
    val langage = stream.filter( word => word.getLang == "fr" )
    //Récupération du text, le nom d'utilisateur, nb de follower et la localisation des twitt écrit en Français
    //Et sauvegarde du DStream.
    val statuses = langage.map(status =>
      "Twitt    : " +
      status.getText          //Text des tweet.
      + "\nUser     : " +
      status.getUser.getName  //Nom d'utilisateur.
      + "\nFollower : " +
      status.getUser.getFollowersCount  //Nombre de follower.
      + "\nLocation : " +
      status.getUser.getLocation        //Localisation lors de l'émission du Twitt.
      + "\n\n").repartition(1)
      //.saveAsTextFiles("/Users/aargancointepas/scala/twitter")//Sauvegarde dans un seul fichier par relever. Mac
        .saveAsObjectFiles("/user/hdfs/scala/twitter")//Sauvegarde dans un seul fichier par relever. HDP
    System.out.println("4")

    <!--Récupération des twitt (ajouter Array dans stream ex : val stream = TwitterUtils.createStrea (ssc, None), Array("#wtf")).-->
    //val status = stream.map( word => word.getText())
    //Sauvegarde les twitt dans le dossier scala
    //status.saveAsTextFiles("/Users/aargancointepas/scala/status")

    <!--Debut de l'enregistrement-->
    ssc.start()
    System.out.println("5")
    //Attente de la fin de l'enregistrement ou après 5min
    ssc.awaitTerminationOrTimeout(300000)

  }
}