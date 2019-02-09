var umqtt = require('./umqtt')
var fs = require('fs')

var logger = { silly: console.log, trace: console.log, info: console.log, debug: console.log, warn: console.log, error: console.log }

var myserver = new umqtt({
    host: "localhost", port: 9010, protocol: 'ws', timeout: 1000000, host: "0.0.0.0", logger: logger
})

myserver.connectAuthenticate = function (data, done) {
    done(true)
}

myserver.clientPublish = function (data, client) {
    if (data.qos == 1) {
        if (!myserver.puback(client.clientId, data.messageId)) {
            console.log('send puback')
        }
    }

}
myserver.setup(function () {
    console.log("finish setup")
    myserver.run()
})




