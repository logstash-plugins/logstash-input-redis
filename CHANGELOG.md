# 2.0.6
  - make integration tests more reliable because of occasional travis failures
# 2.0.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash
# 2.0.4
  - New dependency requirements for logstash-core for the 5.0 release
## 2.0.3
 - Changed default batch size to 125. Improve batch handling code. Add travis ci build with redis integration.

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

## 1.0.3
 - Fix typo in module name in test (Causing CI build failures)

 ## 1.0.2
 - Fix typo in module name (Causing the module to not be loaded)

## 1.0.1
 - Make teardown more reliable
 - Re-organise code and tests
