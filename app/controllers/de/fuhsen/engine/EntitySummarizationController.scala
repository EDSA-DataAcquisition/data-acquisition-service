package controllers.de.fuhsen.engine

import play.Logger
import play.api.mvc.{AnyContent, Action, Controller}

/**
  * Created by dcollarana on 5/23/2016.
  */
class EntitySummarizationController extends Controller {

  def execute = Action {
    request =>  val textBody = request.body.asText
      Logger.info("Entity Summarization")
      Ok(textBody.get)
  }

}
