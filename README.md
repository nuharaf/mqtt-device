mqtt to nats bridge

can be paired with [this mobile app](https://github.com/nuhamind2/android-gps-beacon)

This service accept mqtt message , then forward it into NATS server. Basically a proxy. It is not a complete mqtt broker like mosquitto as it does not do topic matching by itself but instead relegated into NATS server.



