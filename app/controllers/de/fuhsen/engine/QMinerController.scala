package controllers.de.fuhsen.engine

import javax.inject.Inject
import java.util

import com.typesafe.config.ConfigFactory
import play.api.Logger
import org.apache.jena.rdf.model._
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.{RDF, RDFS}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc.{Action, Controller}
import scala.collection.{Seq, mutable}
import play.api.libs.json._
import scala.collection.JavaConversions._
import scala.concurrent.Future
import utils.dataintegration.RDFUtil
/**
  * Created by dcollarana on 5/19/2017.
  */
class QMinerController @Inject()(ws: WSClient) extends Controller {

  val schema = collection.immutable.HashMap(
    "date" ->         "http://schema.org/datePosted",
    "description" ->  "http://schema.org/description",
    "title" ->        "http://schema.org/title",
    "url" ->          "http://schema.org/url",
    "forOrganization" -> "http://schema.org/hiringOrganization",
    "inLocation.name" -> "http://schema.org/jobLocation",
    "inLocation.long" -> "http://www.edsa-project.eu/edsa/wgs84_pos#long",
    "inLocation.lat" -> "http://www.edsa-project.eu/edsa/wgs84_pos#lat",
    "inLocation.uri" -> "http://schema.org/jobLocationUri",
    "requiredSkills" -> "http://www.edsa-project.eu/edsa#requiresSkill",
    "foundIn" -> "http://schema.org/source"
  )

  def send = Action.async {
    //val data = convert2Json("")
    Logger.info("Sending Json value")

    val query =
      s"""
         |PREFIX edsa: <http://www.edsa-project.eu/edsa#>
         |PREFIX sdo: <http://schema.org/>
         |
         |CONSTRUCT ?s ?p ?o
         |where {
         |  GRAPH <http://www.edsa-project.eu/edsa/demand/adzuna> {
         |    ?s a edsa:JobPosting .
         |    ?s ?p ?o .
         |    ?s <http://schema.org/datePosted> ?date .
         |    BIND(strdt(?date, xsd:date) AS ?d)
         |    FILTER (?d > "2017-04-01"^^xsd:date)
         |  }
         |}
          """.stripMargin

    val futureResponse: Future[WSResponse] = for {
      responseOne <- ws.url(ConfigFactory.load.getString("dydra.endpoint.sparql"))
                        .withQueryString("query"-> query)
                        .withHeaders("Accept"->"application/n-triples")
                        .get
      responseTwo <- ws.url(ConfigFactory.load.getString("qminer.endpoint.url"))
                        .post(convertToJson(responseOne.body))
    } yield responseTwo
    //action taken in case of failure
    futureResponse.recover {
      case e: Exception =>
        val exceptionData = Map("error" -> Seq(e.getMessage))
        Logger.error(exceptionData.toString())
    }
    futureResponse.map { response =>
        if (response.status >= 300) {
          InternalServerError(s"${response.status} server error in the service")
        } else
          Ok
    }
  }

  def convertToJson(response:String) : String = {
    val model = RDFUtil.rdfStringToModel(response, Lang.TTL)
    val iterator = model.listSubjects.toList
    var json = JsArray()
    iterator.map { x =>
      var jsObject = processStatement(model, x)
      json =  json :+ jsObject
    }
    Logger.info(json.toString)
    json.toString
  }

  def processStatement(model: Model, subj: Resource): JsValue = {
    val predicates = model.listStatements(subj, null, null).toList
    val date = getPredicateValue(predicates, schema.get("date"))
    val description = getPredicateValue(predicates, schema.get("description"))
    val title = getPredicateValue(predicates, schema.get("title"))
    val url = getPredicateValue(predicates, schema.get("url"))
    val uri = java.util.UUID.randomUUID.toString
    val forOrganization = getPredicateValue(predicates, schema.get("forOrganization"))
    val foundIn = getPredicateValue(predicates, schema.get("foundIn"))
    val skills = getSkills(predicates)
    val location = getLocation(predicates)

      val json: JsValue = Json.obj(
        "date" -> date,
        "description" -> description,
        "title" -> title,
        "url" -> url,
        "uri" -> uri,
        "inLocation" -> location,
        "foundIn" -> Json.obj(
          "name" -> foundIn
        ),
        "requiredSkills" -> skills,
        "forOrganization" -> forOrganization
      )
      json
    }


  def getPredicateValue(predicates: java.util.List[Statement], property:Option[String]):String = {
    var value = ""
    predicates.map { predicate =>
      if(predicate.getPredicate().toString == property.get) value = predicate.getObject().toString
    }
    value
  }

  def getSkills(predicates: java.util.List[Statement]):JsArray = {
    var skills = JsArray()
    predicates.map { predicate =>
      if(predicate.getPredicate().toString == schema.get("requiredSkills").get){
        var jobUri = predicate.getObject().toString
        var jobName = jobUri.substring(33)
        var job = Json.obj(
          "name" -> jobName,
          "uri" -> jobUri
        )
        skills = skills :+ job
      }
    }
    skills
  }

  def getLocation(predicates: java.util.List[Statement]): JsObject = {
    val inLocationName = getPredicateValue(predicates, schema.get("inLocation.name"))
    val inLocationUri = getPredicateValue(predicates, schema.get("inLocation.uri"))
    var inLocationLong = getPredicateValue(predicates, schema.get("inLocation.long")).stripPrefix("\\\"").stripSuffix("\\\"").trim
    var inLocationLat = getPredicateValue(predicates, schema.get("inLocation.lat")).stripPrefix("\\\"").stripSuffix("\\\"").trim
    if(inLocationLong.isEmpty) inLocationLong = "0.0"
    if(inLocationLat.isEmpty) inLocationLat = "0.0"
    val inLocationCoord = Array(inLocationLong.toDouble, inLocationLat.toDouble)
    val json = Json.obj(
      "name" -> inLocationName,
      "coord" -> inLocationCoord,
      "uri" -> inLocationUri
    )
    json
  }

  def receive = Action { request =>
    Logger.info("Starting to receive the Json")
    val json = request.body.asJson
    json match {
      case Some(value) =>
        Logger.info("Json value received")
        Logger.info(value.toString)
        Ok
      case None => BadRequest("No Json Sent!!!")
    }
  }

}

