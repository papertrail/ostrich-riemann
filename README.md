# ostrich-riemann

An [ostrich](https://github.com/twitter/ostrich) reporter for [riemann](https://github.com/aphyr/riemann).

## Usage

In kestrel:

``` scala
import com.papertrailapp.ostrich._

new KestrelConfig {
  // ...

  admin.statsNodes = new StatsConfig {
    reporters = new RiemannStatsLoggerConfig(
      period = 1.second,          // default: 1.minute
      prefix = "kestrel",         // default: None
      host = "riemann01",         // default: localhost
      port = 4444,                // default: 5555
      localHostname = "kestrel01" // default: [local hostname]
    )
  }
}
```
