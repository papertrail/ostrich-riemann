package com.papertrailapp.ostrich

import com.twitter.ostrich.admin.{AdminHttpService, PeriodicBackgroundProcess}
import com.twitter.util.Duration
import com.aphyr.riemann.client.RiemannClient
import com.twitter.ostrich.stats.{StatsListener, StatsCollection}
import java.net.InetAddress
import collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.ostrich.admin.config.{StatsReporterConfig, JsonStatsLoggerConfig}
import com.twitter.logging.Logger

class RiemannStatsLoggerConfig(var period: Duration = 1.minute,
                               var prefix: Option[String] = None,
                               var host: String = "localhost",
                               var localHostname: Option[String] = None,
                               var shortLocalHostname: Boolean = true,
                               var port: Int = 5555,
                               var tags: Seq[String] = Seq())
  extends StatsReporterConfig {

  def apply() = { (collection: StatsCollection, admin: AdminHttpService) =>
    new RiemannStatsLogger(host, port, prefix, localHostname, shortLocalHostname, tags, period, collection)
  }
}

class RiemannStatsLogger(val host: String, val port: Int,
                         val prefix: Option[String],
                         _localHostname: Option[String],
                         val shortLocalHostname: Boolean,
                         var tags: Seq[String] = Seq(),
                         val period: Duration, val collection: StatsCollection)
  extends PeriodicBackgroundProcess("RiemannStatsLogger", period) {

  val logger = Logger.get(getClass.getName)
  val client = RiemannClient.tcp(host, port)
  val listener = new StatsListener(collection)
  val localHostname: String = _localHostname.getOrElse(getLocalHostname)

  def periodic() {
    try {
      client.connect()
      val stats = listener.get()
      val statMap =
        stats.counters.map { case (key, value) => (key, value.doubleValue) } ++
          stats.gauges ++
          stats.metrics.flatMap { case (key, distribution) =>
            distribution.toMap.map { case (subkey, value) =>
              (key + " " + subkey, value.doubleValue)
            }
          }

      val cleanedKeysStatMap = statMap.map { case (key, value) => (keyWithPrefix(key), value) }

      val events = cleanedKeysStatMap.map { case (key, value) =>
        newEvent
          .service(key)
          .metric(value)
          .build()
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
