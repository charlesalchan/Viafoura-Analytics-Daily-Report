package com.viafoura.analytics.report

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.mashape.unirest.http.Unirest
import com.viafoura.analytics.common.DateTimeFunction
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import org.json4s.JsonAST.{JNothing, JNull}
import org.json4s._

import scala.util.Try

/**
 * Created by charles on 27/4/2016.
 */
object Core extends App {
  val scope = "00000000-0000-4000-8000-082f79d5b829/"
  val unique_scope = "00000000-0000-4000-8000-082f79d5b829"
  val url = "http://127.0.0.1:8888/v3/analytics/section/"
  val unique_url = "http://127.0.0.1:8888/v3/analytics/uniques/section/"

  //var catagery = "comments"
  var operation = "/counts/hourly/sum"
  //  implicit val formats = DefaultFormats
  //val result = scala.io.Source.fromURL(url).mkString
  //  println(result)

  //127.0.0.1:8888/v3/analytics/section/00000000-0000-4000-8000-082f79d5b829/comments/counts/hourly/sum?from=1453957200000&to=1454043600000


  def getSearchKey(isReferal: Option[String]) = if (isReferal.isEmpty) "device" else "referral_type"


  var catagerys = Array("comments", "all_logins", "all_signups", "attention_time", "attention-facebook", "attention-google",
    "attention-linkedin", "attention-pinterest", "attention-reddit",
    "attention-referral", "attention-stumbleupon", "attention-twitter",
    "attention-desktop", "attention-mobile", "attention-tablet", "attention-device",
    "page_views", "recirculation")
  var unique_catagerys = Array("unique_view")
  var socialForReferral: Map[String, String] = Map("attention-facebook" -> "facebook", "attention-google" -> "google-plus",
    "attention-linkedin" -> "linkedin", "attention-pinterest" -> "pinterest", "attention-reddit" -> "reddit",
    "attention-stumbleupon" -> "stumbleupon", "attention-twitter" -> "twitter", "attention-referral" -> "referral"
  )
  var deviceForReferral: Map[String, String] = Map("attention-desktop" -> "desktop", "attention-mobile" -> "mobile",
    "attention-tablet" -> "tablet", "attention-device" -> "device"
  )
  var deviceForUniqueDataType: Map[String, String] = Map("unique_view" -> "views"
  )
  var startDate = DateTimeFunction.getLocalTime(2016, 1, 1);


  print("time")
  for (catagery <- catagerys) {
    print(f",$catagery ")
  }
  println("")
  for (a <- 0 to 120) {
    val endDate = startDate.plus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.MILLIS);
    print(startDate)
    for (catagery <- catagerys) {
      //Specal handling for attention time for social media type or device type
      if (socialForReferral.contains(catagery) || (deviceForReferral.contains(catagery))) {
        val searchValue = socialForReferral.get(catagery).orElse(deviceForReferral.get(catagery)).get
        val searchKey = getSearchKey(socialForReferral.get(catagery))

        //println(f"$searchKey : $searchValue")
        var fullUrl = f"$url$scope" + f"attention_time$operation"
        //println(specalForReferral.get(catagery).get);
        val result = Unirest.get(fullUrl)
          //.routeParam("doRequesting UUID main", domain)
          .header("accept-encoding", "") // there's something weird happening with gzip
          .queryString(Map(
          "from" -> startDate.toEpochMilli().toString,
          "to" -> endDate.toEpochMilli.toString,
          searchKey -> searchValue
        ))
          .asString().getBody()
        print(", " + getNumber(result, "sum"));
      } else {
        var fullUrl = f"$url$scope$catagery$operation"
        val result = Unirest.get(fullUrl)
          //.routeParam("doRequesting UUID main", domain)
          .header("accept-encoding", "") // there's something weird happening with gzip
          .queryString(Map(
          "from" -> startDate.toEpochMilli().toString,
          "to" -> endDate.toEpochMilli.toString
        ))
          .asString().getBody()
        print(", " + getNumber(result, "sum"))
      }
    }
      for (catagery <- unique_catagerys) {
        var fullUrl = f"$unique_url$unique_scope"
        val result = Unirest.get(fullUrl)
          //.routeParam("doRequesting UUID main", domain)
          .header("accept-encoding", "") // there's something weird happening with gzip
          .queryString(Map(
          "from" -> startDate.toEpochMilli().toString,
          "to" -> endDate.toEpochMilli.toString,
          "metric" -> deviceForUniqueDataType.get(catagery).get
        ))
          .asString().getBody()

        //println(", " + (result));
        println(", " + getNumberFromUnique(result));
      }
      startDate = startDate.plus(1, ChronoUnit.DAYS)
    }


  def getNumberFromUnique(input : String): String = {
    var result = "0"
    val temp = parse(input).values.asInstanceOf[List[Map[String, Object]]].filter(p = (item: Map[String, Object]) => {
      "all".equals(item.get("referral_type").get)
    }
    ).foreach( item => result = item.get("count").get.toString)
    return result
//    "0"
    //return temp.;
  }
  def getNumber(input:String,  key : String ):String = {
//    println(input)
//    println( parse(input) \ "sum" )
//
//    val value = JsonMethods.parse(input)
//
//    value \ "sum" match{
//      case JNothing | JNull => println("nothing or null")
//      case value => println(value.values.toString)
//    }
    val result = Try((parse(input) \ "sum").values.toString ) //.extract[Int]).getOrElse(1)//try((parse(input) \ key).extract([Long])).getOrElse(0L)
    result.get
  }
}
