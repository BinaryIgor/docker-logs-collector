# Docker logs collector

Lightweight docker logs collector.

It periodically collects logs from containers running on a given machine,
and sends them to an arbitrary http endpoint.

It is designed to be resilient, in particular:
* It tries to connect to docker until success (indefinitely)
* It tries to reconnect in the same manner when connection is lost
* It always tries to send data to configured endpoint a few times (configurable)

Nevertheless, in a production it should be wrapped in some script that will make sure that this process is always running.