import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import GabJsonProtocol.GabPostJsonFormat
object GabParser extends App{
  val conf = new SparkConf()
//  conf.set("spark.driver.extraClassPath","/usr/hdp/current/hadoop-client/lib/snappy*.jar")
//  conf.set("spark.driver.extraLibraryPath","/usr/hdp/current/hadoop-client/lib/native")
  val sc = new SparkContext(conf)
  val rawposts = sc.textFile("/data/gabParsed")
  val posts = rawposts.map(raw => {
    try{
      val post = raw.parseJson.convertTo[GabPost]
        //.drop(raw.indexOf("\"b'")+3).dropRight(3)
        //.replaceAll("(?<![\\[\\:\\{\\,])\\\"(?![\\:\\}\\,])","\\\"")

//        .replaceAll("\\\\","")
                  //.drop(x.indexOf("\"b'")+3).dropRight(3).replaceAll("\\\\","")
//        .replaceAll(""""body":(.*)"body_html":""",""""body_html":""").replaceAll(""""embed":(.*)"attachment":""",""""attachment":""").replaceAll(""""attachment":(.*)},"category":""",""""category":""")

      val id = post.id.get
      val time = post.created_at.get
      var userID = -1
      //post.user match {
      //  case Some(user) => userID = user.id
      //  case None =>
     // }
      var topicID = ""
     // post.topic match {
     //   case Some(topic) => topicID = topic.id
     //   case None =>
      //}
      var parentID = -1L
      var parentUserID = -1
     // post.parent match {
     //   case Some(parent) => {
     //     parentID = parent.id.get
      //    parent.user match {
      //      case Some(user) =>  parentUserID = user.id
     //       case None =>
     //     }
     //   }
    //    case None =>
    //  }
      //s"$time;$id;$userID;$topicID;$parentID;$parentUserID"
      "passed"
    }
    catch {
      case e:Exception => raw
    }
  })
  posts.filter(x => !x.equals("passed")).saveAsTextFile("gabFinal")
  //posts.filter(x => x.equals("failed")).saveAsTextFile("gabFinalFailed")
}