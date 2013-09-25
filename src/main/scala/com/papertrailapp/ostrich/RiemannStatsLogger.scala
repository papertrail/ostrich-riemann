package com.papertrailapp.ostrich

import com.twitter.ostrich.admin.{AdminHttpService, PeriodicBackgroundProcess}
import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.aphyr.riemann.client.RiemannClient
import com.twitter.ostrich.stats.{StatsListener, StatsCollection}
import java.net.InetAddress
import collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.ostrich.admin.config.{StatsReporterConfig, JsonStatsLoggerConfig}

class RiemannStatsLoggerConfig(var period: Duration = 1.minute,
                               var prefix: Option[String] = None,
                               var host: String = "localhost",
                               var localHostname: Option[String] = None,
                               var shortLocalHostname: Boolean = true,
                               var port: Int = 5555)
  extends StatsReporterConfig {

  def apply() = { (collection: StatsCollection, admin: AdminHttpService) =>
    new RiemannStatsLogger(host, port, prefix, localHostname, shortLocalHostname, period, collection)
  }
}

class RiemannStatsLogger(val host: String, val port: Int, val prefix: Option[String],
                         _localHostname: Option[String],
                         val shortLocalHostname: Boolean,
                         val period: Duration, val collection: StatsCollection)
  extends PeriodicBackgroundProcess("RiemannStatsLogger", period) {

  val client = RiemannClient.tcp(host, port)
  val listener = new StatsListener(collection)
  val localHostname: String = _localHostname.getOrElse(getLocalHostname)

  def periodic() {
    new JsonStatsLoggerConfig
    client.connect()
    val stats = listener.get()
    val statMap =
      stats.counters.map { case (key, value) => (key, value.doubleValue) } ++
        stats.gauges ++
        stats.metrics.flatMap { case (key, distribution) =>
          distribution.toMap.map { case (subkey, value) =>
            (key + "." + subkey, value.doubleValue)
          }
        }


    val cleanedKeysStatMap = statMap.map { case (key, value) =>
      (keyWithPrefix(key).replaceAll(":", "_").replaceAll("/", ".").replaceAll(" ", "_").toLowerCase(), value) }

    val events = cleanedKeysStatMap.map { case (key, value) =>
      client.event()
        .service(key)
        .host(localHostname)
        .metric(value)
        .ttl(period.inSeconds * 5)
        .build()
    }

    client.sendEventsWithAck(events.toList)
  }

  def keyWithPrefix(key: String): String = {
    prefix.map { p => "%s.%s".format(p, key) }.getOrElse(key)
  }

  def getLocalHostname: String = {
    val localHostname = InetAddress.getLocalHost.getHostName

    if (shortLocalHostname) {
      val regex = """^([^\.]+)""".r
      regex.findFirstIn(localHostname).getOrElse(localHostname)
    } else {
      localHostname
    }
  }
}
