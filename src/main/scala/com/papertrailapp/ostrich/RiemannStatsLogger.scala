package com.papertrailapp.ostrich

import java.util.concurrent.{TimeUnit, ScheduledFuture}

import com.aphyr.riemann.Proto
import com.twitter.ostrich.admin.{Service, AdminHttpService, PeriodicBackgroundProcess}
import com.aphyr.riemann.client.RiemannClient
import com.twitter.ostrich.stats.{Distribution, StatsListener, StatsCollection}
import java.net.InetAddress
import collection.JavaConversions._
import com.twitter.util.{Time, Duration}
import com.twitter.conversions.time._
import com.twitter.ostrich.admin.config.{StatsReporterConfig, JsonStatsLoggerConfig}
import com.twitter.logging.Logger

class RiemannStatsLoggerConfig(period: Duration = 1.minute,
                               prefix: Option[String] = None,
                               host: String = "localhost",
                               localHostname: Option[String] = None,
                               shortLocalHostname: Boolean = true,
                               port: Int = 5555,
                               tags: Seq[String] = Seq(),
                               percentiles: Seq[Double] = Seq(0.50, 0.75, 0.95, 0.99),
                               ttl: Duration = null)
  extends StatsReporterConfig {

  def apply() = { (collection: StatsCollection, admin: AdminHttpService) =>
    new RiemannStatsLogger(host, port, prefix, localHostname, shortLocalHostname, tags, percentiles, period, collection, ttl)
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
                         val collection: StatsCollection,
                         _ttl: Duration = null) extends Service {

  val logger = Logger.get(getClass.getName)
  val riemann = RiemannClient.tcp(host, port)
  val listener = new StatsListener(collection)
  val localHostname: String = _localHostname.getOrElse(getLocalHostname)
  var schedule: ScheduledFuture[_] = null

  val ttl: Float = {
    if (_ttl != null) {
      _ttl.inSeconds
    } else {
      period.inSeconds * 5
    }
  }

  def start() = synchronized {
    schedule = riemann.scheduler().every(period.inNanoseconds, period.inNanoseconds, TimeUnit.NANOSECONDS,
      new Runnable() { override def run() = report })
  }

  def shutdown() = synchronized {
    if (schedule != null) {
      schedule.cancel(true)
      schedule = null
    }

    report
    riemann.close()
  }

  def report() {
    try {
      riemann.connect()

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

      val promise = riemann.sendEvents(events.toList)

      if (promise.deref(period.inNanoseconds, TimeUnit.NANOSECONDS) == null) {
        logger.warning("Timeout after %s while submitting riemann metrics", period)
        riemann.close()
      }
    } catch {
      case ex: Throwable => {
        logger.warning(ex, "Could not submit riemann metrics")
        riemann.close()
      }
    }
  }

  def newEvent = {
    val event = riemann.event()

    event.host(localHostname)
    event.ttl(ttl)
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
