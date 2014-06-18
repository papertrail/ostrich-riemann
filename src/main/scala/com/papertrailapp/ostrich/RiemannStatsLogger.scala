package com.papertrailapp.ostrich

import com.aphyr.riemann.Proto
import com.twitter.ostrich.admin.{AdminHttpService, PeriodicBackgroundProcess}
import com.aphyr.riemann.client.RiemannClient
import com.twitter.ostrich.stats.{Distribution, StatsListener, StatsCollection}
import java.net.InetAddress
import collection.JavaConversions._
import com.twitter.util.{Time, Duration}
import com.twitter.conversions.time._
import com.twitter.ostrich.admin.config.{StatsReporterConfig, JsonStatsLoggerConfig}
import com.twitter.logging.Logger

class RiemannStatsLoggerConfig(var period: Duration = 1.minute,
                               var prefix: Option[String] = None,
                               var host: String = "localhost",
                               var localHostname: Option[String] = None,
                               var shortLocalHostname: Boolean = true,
                               var port: Int = 5555,
                               var tags: Seq[String] = Seq(),
                               val percentiles: Seq[Double] = Seq(0.50, 0.75, 0.95, 0.99))
  extends StatsReporterConfig {

  def apply() = { (collection: StatsCollection, admin: AdminHttpService) =>
    new RiemannStatsLogger(host, port, prefix, localHostname, shortLocalHostname, tags, percentiles, period, collection)
  }
}

class RiemannStatsLogger(val host: String,
                         val port: Int,
                         val prefix: Option[String],
                         _localHostname: Option[String],
                         val shortLocalHostname: Boolean,
                         val tags: Seq[String] = Seq(),
                         val percentiles: Seq[Double] = Seq(0.50, 0.75, 0.95, 0.99),
                         val period: Duration,
                         val collection: StatsCollection)
  extends PeriodicBackgroundProcess("RiemannStatsLogger", period) {

  val logger = Logger.get(getClass.getName)
  val client = RiemannClient.tcp(host, port)
  val listener = new StatsListener(collection)
  val localHostname: String = _localHostname.getOrElse(getLocalHostname)
  var lastDeadline = Time.now

  override def nextRun: Duration = {
    val now = Time.now

    if (lastDeadline < now) {
      lastDeadline = lastDeadline + period
    }

    var duration = lastDeadline - now

    if (duration < 0) {
      lastDeadline = Time.fromMilliseconds((now.inMilliseconds / period.inMilliseconds) * period.inMilliseconds)
    }

    duration
  }

  def periodic() {
    try {
      client.connect()

      val stats = listener.get()
      var events = List[Proto.Event]()

      for ((key, value) <- stats.counters) {
        events ::= newEvent
          .service(keyWithPrefix(key))
          .metric(value.doubleValue)
          .tags("ostrich", "ostrich-count")
          .build()
      }

      for ((key, value) <- stats.gauges) {
        events ::= newEvent
          .service(keyWithPrefix(key))
          .metric(value.doubleValue)
          .tags("ostrich")
          .build()
      }

      for ((key, distribution) <- stats.metrics) {
        events ::= newEvent
          .service(keyWithPrefix(key + " count"))
          .metric(distribution.count)
          .tags("ostrich", "ostrich-count")
          .build()

        val histogram = distribution.histogram

        for (percentile <- percentiles) {
          val percentileName = percentileToName(percentile)

          events ::= newEvent
            .service(keyWithPrefix(key + " " + percentileName))
            .metric(histogram.getPercentile(percentile))
            .tags("ostrich", "ostrich-" + percentileName)
            .build()
        }
      }

      client.sendEventsWithAck(events.toList)
    } catch {
      case ex: Throwable => logger.warning("Could not submit riemann metrics", ex)
    }
  }

  def newEvent = {
    val event = client.event()

    event.host(localHostname)
    event.ttl(period.inSeconds * 5)
    event.tags(tags)

    event
  }

  def percentileToName(percentile: Double) = {
    """^0\.(\d{2}[^0]*).*$""".r.replaceAllIn("%f".format(percentile), "p$1")
  }

  def keyWithPrefix(key: String): String = {
    prefix.map { p => "%s%s".format(p, key) }.getOrElse(key)
  }

  def getLocalHostname: String = {
    val localHostname: String = InetAddress.getLocalHost.getHostName

    if (shortLocalHostname) {
      val regex = """^([^\.]+)""".r
      regex.findFirstIn(localHostname).getOrElse(localHostname)
    } else {
      localHostname
    }
  }
}
