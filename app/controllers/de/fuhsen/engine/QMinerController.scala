package controllers.de.fuhsen.engine

import javax.inject.Inject

import com.typesafe.config.ConfigFactory
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future

/**
  * Created by dcollarana on 5/19/2017.
  */
class QMinerController @Inject()(ws: WSClient) extends Controller {

  def send = Action.async {
    //val data = convert2Json("")
    Logger.info("Sending Json value")

    val query = s"""
         |construct ?s ?p ?o
         |where { GRAPH <http://www.edsa-project.eu/edsa/demand/adzuna> {
         | ?s ?p ?o .
         | ?s <http://schema.org/datePosted> ?date .
         | FILTER (strdt(?date, xsd:dateTime) > "2017-05-17T00:00:00Z"^^xsd:dateTime)
         |} }
          """.stripMargin

    val futureResponse: Future[WSResponse] = for {
      responseOne <- ws.url(ConfigFactory.load.getString("dydra.endpoint.sparql"))
                        .withQueryString("query"-> query)
                        .withHeaders("Accept"->"application/n-triples")
                        .get
      responseTwo <- ws.url(ConfigFactory.load.getString("qminer.endpoint.url"))
                        .post(convert2Json(responseOne.body))
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

    /*
    ws.url(ConfigFactory.load.getString("qminer.endpoint.url"))
      .post(data)
      .map( response =>
        if (response.status >= 300) {
          InternalServerError(s"${response.status} server error in the service")
        } else
          Ok
      )
    */
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

  private def convert2Json(model: String): JsValue = {
    Logger.info("model: "+model)
    val json: JsValue = Json.obj(
      "date" -> "2017-10-02T05:24:32Z",
      "description" -> "... availability and consistency for all stakeholders",
      "title" -> "Data Warehouse Engineer Master Blaster",
      "uri" -> "some uri",
      "url" -> "some url",
      "inLocation" -> Json.obj(
        "name" -> "some name",
        "coord" -> "1234",
        "uri" -> "http://sws.geonames.org/2921055"
      ),
      "inCountry" -> Json.obj(
        "name" -> "Germany",
        "coord" -> "54321",
        "uri" -> "http://sws.geonames.org/2921044"
      ),
      "foundIn" -> Json.obj(
        "name" -> "Adzuna"
      ),
      "requiredSkills" -> Json.arr(
        Json.obj(
          "name" -> "data warehouse",
          "uri" -> "http://www.edsa-project.eu/skill/data warehouse"
        ),
        Json.obj(
          "name" -> "sql",
          "uri" -> "http://www.edsa-project.eu/skill/sql"
        )
      ),
      "forOrganization" -> Json.obj(
        "title" -> "Blacklane GmbH"
      )
    )
    json
  }

}

