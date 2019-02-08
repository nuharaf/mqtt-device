var umqtt = require('./umqtt')
var fs = require('fs')

cert = fs.readFileSync("selfsigned2.crt")
key = fs.readFileSync("selfsigned2.key")

var logger = { silly: console.log, trace: console.log, info: console.log, debug: console.log, warn: console.log, error: console.log }

var myserver = new umqtt({
    port: 9000, protocol: 'ws', protocolOpts: { key: key, cert: cert }
    , host: "localhost", logger: logger
})

myserver.connectAuthenticate = function (data, done) {
    done(true)
}

myserver.clientPublish = function (data, client) {
    if (data.qos == 1) {
        setTimeout(function(){
            if(!myserver.puback(client.clientId, data.messageId)){
            }
        },1000)
        
    }

}
myserver.setup(function () {
    console.log("finish setup")
    myserver.run()
})




