package controllers.de.fuhsen.crawling

import java.util.concurrent.atomic.AtomicLong
import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import controllers.de.fuhsen.crawling.CrawlerActor.StartCrawl
import play.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc.{Action, Controller, Result}
import JsonFormatters._

import scala.concurrent.Future

/**
  * Handles REST request for the crawl service.
  */
@Singleton
class CrawlerController @Inject()(ws: WSClient, system: ActorSystem) extends Controller {
  val crawlActor = system.actorOf(CrawlerActor.props(ws), "crawler-actor")

  /**
    * Returns all jobs of all crawls.
    */
  def crawlJobs = Action.async { request =>
    nutchGetRequest(ConfigFactory.load().getString("crawler.nutch.rest.api.url") + "/job") { response =>
      Ok(response.json)
    } map pickResult
  }

  /**
    * Returns all jobs of a specific crawl.
    *
    * @param crawlId
    */
  def crawlJobsByCrawlId(crawlId: String) = Action.async { request =>
    nutchGetRequest(ConfigFactory.load().getString("crawler.nutch.rest.api.url") + "/job") { response =>
      response.json match {
        case jobs: JsArray =>
          Ok(JsArray(jobs.value.filter(v => (v \ "crawlId").as[String] == crawlId)))
        case other =>
          InternalServerError("Expected JSON array from Nutch service. But got " + other.getClass.getName)
      }
    } map pickResult
  }

  /**
    * Returns only the crawl job progress of a specific crawl.
    *
    * @param crawlId
    */
  def crawlJobProgressByCrawlId(crawlId: String) = Action.async { request =>
    handleNutchJobProgress(crawlId) { jobsReducedInformation =>
      Ok(JsArray(jobsReducedInformation.map(Json.toJson(_))))
    }
  }

  /**
    * Returns the status of the crawl.
    *
    * @param crawlId
    */
    def crawlStatusByCrawlId(crawlId: String) = Action.async { request =>
      handleNutchJobProgress(crawlId) { jobsReducedInformation =>
        jobsReducedInformation.filter(j => j.`type` == "INDEX" || j.status != "FINISHED").headOption match {
          case Some(current) =>
            val currentStatus = (current.`type`, current.status) match {
              case ("INDEX", "FINISHED") =>
                "FINISHED"
              case (_, "FAILED") =>
                "FAILED"
              case (_, state) =>
                state
            }
            val crawlStatus = CrawlProgress(crawlId, current.`type`.toString, currentStatus)
            Ok(Json.toJson(crawlStatus))
          case _ =>
            NotFound
        }

      }
    }

  private def handleNutchJobProgress(crawlId: String)(handle: Seq[CrawlJobProgress] => Result): Future[Result] = {
    nutchGetRequest(ConfigFactory.load().getString("crawler.nutch.rest.api.url") + "/job") { response =>
      response.json match {
        case jobs: JsArray =>
          val jobsReducedInformation = reduceNutchJobsToProgressData(crawlId, jobs)
          handle(jobsReducedInformation)
        case other =>
          InternalServerError("Expected JSON array from Nutch service. But got " + other.getClass.getName)
      }
    } map pickResult
  }

  private def reduceNutchJobsToProgressData(crawlId: String, jobs: JsArray): Seq[CrawlJobProgress] = {
    val jobsByCrawlId = jobs.value.filter(v => (v \ "crawlId").as[String] == crawlId)
    val jobsReducedInformation = jobsByCrawlId map { job =>
      val id = (job \ "id").as[String]
      val msg = (job \ "msg").as[String]
      val `type` = (job \ "type").as[String]
      val status = (job \ "state").as[String]
      CrawlJobProgress(id, msg, `type`, status)
    }
    jobsReducedInformation
  }

  private def pickResult(successOrError: Either[Result, Result]): Result = {
    successOrError match {
      case Left(result) => result
      case Right(result) => result
    }
  }

  /** Helper method for GET requests */
  def nutchGetRequest[T](url: String, queryParameters: Map[String, String] = Map(), timeoutInMs: Long = 10000)(processing: (WSResponse) => T): Future[Either[T, Result]] = {
    val jobsResponse = ws.url(url).
        withRequestTimeout(timeoutInMs).
        withQueryString(queryParameters.toSeq: _*).
        get()
    jobsResponse map { response =>
      handleHttpErrors(response, processing)
    }
  }

  /** Helper method for POST requests */
  def nutchPostRequest[T](url: String,
                          postBody: JsValue,
                          queryParameters: Map[String, String] = Map(),
                          timeoutInMs: Long = 10000)
                         (processing: (WSResponse) => T): Future[Either[T, Result]] = {
    val jobsResponse = ws.url(url).
        withRequestTimeout(timeoutInMs).
        withQueryString(queryParameters.toSeq: _*).
        post(postBody)
    jobsResponse map { response =>
      handleHttpErrors(response, processing)
    }
  }

  /** Handles HTTP errors in a default way. */
  def handleHttpErrors[T](response: WSResponse,
                          processing: (WSResponse) => T): Either[T, Result] = {
    response.status / 100 match {
      case 2 =>
        Left(processing(response))
      case 3 =>
        ???
      case 4 =>
        Right(InternalServerError("Nutch request seems to be wrong. Nutch REST service returned " + response.status + "\nMessage: " + response.statusText))
      case _ =>
        Right(InternalServerError("Nutch REST service returned status" + response.status + "\nMessage: " + response.statusText))
    }
  }

  private def createNutchJob(crawlId: String, json: JsValue): Future[Either[String, Result]] = {
    nutchPostRequest(ConfigFactory.load().getString("crawler.nutch.rest.api.url") + "/job/create", json) { response =>
      response.body
    }
  }

  private def createSeedList(crawlId: String, urls: Seq[String]): Future[Either[String, Result]] = {
    val createSeedList = seedListRequest(urls)
    val postBody = Json.toJson(createSeedList)
    nutchPostRequest(
      ConfigFactory.load().getString("crawler.nutch.rest.api.url") + "/seed/create",
      postBody
    ) { result =>
      result.body
    }
  }

  private def seedListRequest(urls: Seq[String]): CreateSeedListBody = {
    CreateSeedListBody(
      id = CrawlCounter.seedListIdCounter.getAndIncrement().toString,
      name = "nutch",
      seedUrls = urls.zipWithIndex.map { case (url, count) =>
        SeedUrl(
          id = count + 1,
          seedList = null,
          url = url
        )
      }
    )
  }

  /**
    * Creates a new crawl job given some URIs. Request JSON format:
    *
    * <pre>
    * { "seedURLs": [
    * "http://url1.com",
    * "http://url2.com",
    * ...
    * ]}
    * </pre>
    *
    * @return The crawl id of the crawl job.
    */
  def createCrawlJob() = Action.async { request =>
    Logger.info("Creating crawl jobs...")
    request.body.asJson match {
      case Some(json) =>
        (json \ "seedURLs") match {
          case JsDefined(urlsJs: JsArray) =>
            val crawlId = CrawlCounter.getNextCrawlId()
            val urls = urlsJs.value map (jsString => jsString.as[String])
            createSeedListAndHandleResponse(crawlId, urls)
          case other =>
            Future(BadRequest("Wrong JSON format! Property seedURLs must be an array. But it has been " + other.getClass.getName))
        }
      case None =>
        Future(NotAcceptable("Valid content types: text/json, application/json"))
    }
  }

  private def createSeedListAndHandleResponse(crawlId: String, urls: Seq[String]): Future[Result] = {
    createSeedList(crawlId, urls) map { result =>
      result match {
        case Left(seedListPath) =>
          crawlActor ! StartCrawl(crawlId, seedListPath)
          Logger.info(s"Created crawl $crawlId")
          val path = "/crawling/jobs/" + crawlId
          Created(path).withHeaders(LOCATION -> path)
        case Right(failureResult) =>
          failureResult
      }
    }
  }
}

/**
  * Generates IDs for crawl jobs.
  */
object CrawlCounter {
  val seedListIdCounter = new AtomicLong(1)

  val crawlIdCounter = new AtomicLong(1)

  def getNextCrawlId(): String = {
    val count = crawlIdCounter.getAndIncrement()
    val time = System.currentTimeMillis()
    val crawlId = s"crawl-$time-$count"
    crawlId
  }
}