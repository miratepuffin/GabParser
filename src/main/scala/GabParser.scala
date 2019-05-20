import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import GabJsonProtocol.GabPostJsonFormat
object GabParser extends App{
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val rawposts = sc.textFile("/data/gabFull")
  rawposts.map(raw => {
    //try{
      val post = raw
                  .drop(raw.indexOf("\"b'")+3)
                  .dropRight(3)
                  .replaceAll("\\\\","")
                  .parseJson.convertTo[GabPost]
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
      s"$time;$id;$userID;$topicID;$parentID;$parentUserID"
    //}
    //catch {
    //  case e:Exception => "failed"
   // }
  }).filter(x => !x.equals("failed")).saveAsTextFile("gabFinal")
}

object GabEntityType extends Enumeration {
  val post : Value = Value("post")
  val user : Value = Value("user")
  val topic: Value = Value("topic")
}