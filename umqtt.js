var mqttCon = require('mqtt-connection')
var net = require('net')
var tls = require('tls');
var http = require('http')
var https = require('https')
var WebSocket = require('ws')
var websocketStream = require('websocket-stream')
var async = require('async')

var CONNECT_PACKET = Symbol('CONNECT')
var CONNECT_TIMESTAMP = Symbol('CONNECT_TIMESTAMP')
var PACKET_TIMESTAMP = Symbol('PACKET_TIMESTAMP')
var WS_REQUEST = Symbol('WSREQ')
var DISCONNECT_PACKET = Symbol('DISCONNECT')

class umqtt {
    constructor(opts) {
        this.port = opts.port
        this.protocol = opts.protocol
        this.protocolOpts = opts.protocolOpts
        this.host = opts.host
        this.logger = opts.logger
        this.timeout = opts.timeout ? opts.timeout : 10000
        this.sameId = opts.sameId === "disconnect" ? "disconnect" : "prevent"
        this.clientPublish = function () { }
        this.connectAuthenticate = function (data, cb) { cb(true) }
        this.clientSubscribe = function (data, client, cb) { cb(true) }
        this.clientUnsubscribe = function (data, client) { }
        this.clientDisconnect = function (client) { }
        this.metric = {}
        this.metric.publishCounter = { inc: function () { } }
        this.metric.publishMessageHist = { observe: function () { } }
        this.metric.activeClientGauge = { inc: function () { }, dec: function () { } }
        //this.clientList = {}
        this.clientMap = new Map()
        this.connectHook = function () { }

        this.paused = false
    }

    setMetric(metrics) {

    }

    getClientList() {
        let clients = {}
        for (let conn of this.clientMap.values()) {
            //let conn = this.clientList[key]
            let info = {}
            info.socket = this._socketGetTuple(conn.stream)
            info.connectTime = conn[CONNECT_TIMESTAMP]
            info.lastMessage = (new Date(conn[PACKET_TIMESTAMP])).toUTCString()
            if (this.protocol === 'wss' || this.protocol === 'ws') {
                info.http = this._wsGetInfo(conn.stream[WS_REQUEST])
                info.socket = this._socketGetTuple(conn.stream[WS_REQUEST].connection)
            }
            else {
                info.socket = this._socketGetTuple(conn.stream)
            }
            //clients[key] = info          
            clients[conn[CONNECT_PACKET].clientId] = info
        }
        return clients
    }

    _verifyConfig() {
        if (this.protocol !== "ws" && this.protocol !== "tcp" &&
            this.protocol !== "wss" && this.protocol !== "tls") {
            return false;
        }
        if (this.protocol === "wss" || this.protocol === "tls") {
            if (this.protocolOpts == undefined) {
                return false;
            }
        }
        return true
    }

    setup(done) {
        var self = this
        if (!self._verifyConfig()) {
            return false;
        }
        if (self.protocol === "ws") {
            const httpServer = http.createServer()
            const wss = new WebSocket.Server({ server: httpServer });
            httpServer.listen(self.port, self.host,
                function () {
                    self.listening = true
                    self.logger.info(`HTTP server listening on ${httpServer.address().address}:${httpServer.address().port}`)
                    if (done) { done() }
                }.bind(self))
            self.websocketServer = wss
        }
        else if (self.protocol === "wss") {
            const httpsServer = https.createServer({
                key: self.protocolOpts.key,
                cert: self.protocolOpts.cert
            })
            const wss = new WebSocket.Server({ server: httpsServer });
            httpsServer.listen(self.port, self.host,
                function () {
                    self.listening = true
                    self.logger.info(`HTTPS server listening on ${httpsServer.address().port}`)
                    if (done) { done() }
                }.bind(self))
            self.websocketServer = wss
        }
        else if (self.protocol === 'tcp') {
            const server = new net.Server()
            server.listen(self.port,
                function () {
                    self.listening = true
                    self.logger.info(`TCP server listening on ${server.address().port}`)
                    self.socketServer = server
                    if (done) { done() }
                }.bind(self))
            self.socketServer = server
        }
        else if (self.protocol === "tls") {
            const server = new tls.Server()
            server.setSecureContext({
                key: self.protocolOpts.key,
                cert: self.protocolOpts.cert
            })
            server.listen(self.port,
                function () {
                    self.listening = true
                    self.logger.info(`TLS server listening on ${server.address().port}`)
                    self.socketServer = server
                    if (done) { done() }
                }.bind(self))
            self.socketServer = server
        }
        else {
            return false;
        }
        return true;
    }

    _evictConnection(conn, time) {
        var self = this
        return setTimeout(function () {
            self.logger.warn("Connection not sending CONNECT packet within timeout")
            conn.end()
            conn.destroy()
        }.bind(self), time)
    }

    _attachPurgerTrigger() {
        var self = this
        self.connectHook = function () {
            self.logger.debug(`Purger scheduled`)
            setTimeout(self._purger.bind(self), 1000)
            self.connectHook = function () { }
        }
    }

    _purger() {
        var self = this
        var smallestKeepalive = undefined
        self.logger.debug(`Running client purger`)
        var count = self.clientMap.size;
        if (count == 0) {
            self.logger.info(`No client to purge`)
            self._attachPurgerTrigger()
            return
        }
        var purgedCount = 0
        let checkPacketOverdue = function (conn) {
            let connPkt = conn[CONNECT_PACKET]
            let keepalive = connPkt.keepalive
            if(smallestKeepalive == undefined){
                smallestKeepalive = keepalive
            }
            else if (keepalive < smallestKeepalive){
                smallestKeepalive = keepalive
            }
            let lastMessage = conn[PACKET_TIMESTAMP]
            let now = Date.now()
            if ((lastMessage + 1500 * keepalive) < now) {
                conn.end()
                conn.destroy()
                self.logger.info(`ClientId ${connPkt.clientId} purged, overdue by ${now - (lastMessage + 1000 * keepalive)} miliseconds`)
                purgedCount++
            }
        }

        let reschedule = function () {
            let newTimeout = smallestKeepalive == undefined ? 10000 : smallestKeepalive * 500
            self.logger.debug(`${purgedCount > 0 ? purgedCount : 'No'} client purged from ${count} client`)
            self.logger.debug(`Reschedule client purging for the next ${newTimeout / 1000} second`)
            setTimeout(self._purger.bind(self), newTimeout)
        }
        for (let conn of self.clientMap.values()) {
            checkPacketOverdue(conn)
        }
        reschedule()
        // async.each(self.clientMap.values(), function (conn, done) {
        //     let connPkt = conn[CONNECT_PACKET]
        //     let keepalive = connPkt.keepalive
        //     if (keepalive < smallestKeepalive) {
        //         smallestKeepalive = keepalive
        //     }
        //     let lastMessage = conn[PACKET_TIMESTAMP]
        //     let now = Date.now()
        //     if ((lastMessage + 1500 * keepalive) < now) {
        //         conn.end()
        //         conn.destroy()
        //         self.logger.info(`ClientId ${connPkt.clientId} purged, overdue by ${now - (lastMessage + 1000 * keepalive)} miliseconds`)
        //         purgedCount++
        //     }
        //     done()
        // }, function () {
        //     let newTimeout = smallestKeepalive == 1000 ? 10000 : smallestKeepalive * 500
        //     self.logger.debug(`${purgedCount > 0 ? purgedCount : 'No'} client purged from ${count} client`)
        //     self.logger.debug(`Reschedule client purging for the next ${newTimeout / 1000} second`)
        //     setTimeout(self._purger.bind(self), newTimeout)
        // })
    }

    run() {
        var self = this
        if (self.listening != true) {
            self.logger.error("Server not ready")
            return false
        }
        if (self.protocol === "ws" || self.protocol === "wss") {
            self.websocketServer.on("connection", function (ws, req) {
                var stream = websocketStream(ws)
                stream[WS_REQUEST] = req
                self.logger.silly(`Request header : ${JSON.stringify(req.headers)}`)
                self._handler(stream)
            }.bind(self))
        }
        else if (self.protocol === "tcp") {
            self.socketServer.on("connection", self._handler.bind(self))
        }
        else if (self.protocol === "tls") {
            self.socketServer.on("secureConnection", self._handler.bind(self))
        }
        self._attachPurgerTrigger()
    }

    pause(){
        this.paused = true
    }

    publish(clientId, message) {
        var self = this
        self.logger.silly(`Publish message with topic ${message.topic} to ${clientId}`)
        var conn = self.clientMap.get(clientId)
        if (conn == undefined) {
            self.logger.warn(`ClientId ${clientId} not connected`)
            return false
        }
        if (message.topic === undefined) {
            message.topic = ''
        }
        conn.publish(message)
        return true
    }

    puback(clientId, messageId) {
        var self = this
        //var conn = self.clientList[clientId]
        var conn = self.clientMap.get(clientId)
        if (conn == undefined) {
            return false
        }
        conn.puback({ messageId: messageId })
        return true;
    }

    disconnect(clientId) {
        var self = this
        //var conn = self.clientList[clientId]
        var conn = self.clientMap.get(clientId)
        if (conn !== undefined) {
            conn.end()
            conn.destroy()
            return true
        }
        else {
            return false;
        }
    }

    _socketGetTuple(socket) {
        return {
            remote: [socket.remoteAddress, socket.remotePort],
            local: [socket.localAddress, socket.localPort]
        }
    }

    _wsGetInfo(req) {
        return {
            xrealip : req.headers['x-real-ip'],
            XFF: req.headers['x-forwarded-for'],
            Host: req.headers['host'],
            Url: req.url
        }
    }

    _onConnectHandler(connObj, packet) {
        var self = this
        if (self.paused){
            return
        }
        self.testconn = connObj
        self.logger.debug(`Connect attempt with clientId ${packet.clientId}`)
        if(packet.clientId == ""){
            self.logger.error(`ClientId empty`)
            return
        }
        if(packet.keepalive == 0){
            self.logger.error(`Keepalive set to zero`)
            return
        }
        self.logger.debug(`Connect attempt keepalive : ${packet.keepalive}`)
        var exist = self.clientMap.has(packet.clientId)
        if (exist && self.sameId === "disconnect") {
            //disconnect current connection
            let connObj = self.clientMap.get(packet.clientId)
            connObj.end()
            connObj.destroy()
            self.logger.debug(`Existing connection with clientid ${packet.clientId} evicted`)
        }
        else if (exist && self.sameId === "prevent") {
            //refuse connection with same clientId
            self.logger.warn(`Connection with clientId of ${packet.clientId} already exist`)
            self.logger.info(`ClientId ${packet.clientId} connect attempt rejected`)
            connObj.end()
            connObj.destroy()
            return
        }
        //call user provided auth handler
        self.connectAuthenticate({
            clientId: packet.clientId,
            username: packet.username,
            password: packet.password
        }, function (connObj, packet, res) {
            if (res) {
                //auth accepted
                if (self.clientMap.has(packet.clientId)) {
                    //check again because clientMap might change before this handler called
                    self.logger.warn(`Connection with clientId of ${packet.clientId} already exist`)
                    self.logger.info(`ClientId ${packet.clientId} connect attempt rejected`)
                    connObj.end();
                    connObj.destroy();
                    return;
                }
                connObj.connack({ returnCode: 0 })
                self.clientMap.set(packet.clientId, connObj)
                connObj[CONNECT_PACKET] = packet
                connObj[CONNECT_TIMESTAMP] = new Date() //attach timestamp
                connObj[PACKET_TIMESTAMP] = Date.now()
                self.connectHook()
                self.logger.info(`ClientId ${packet.clientId} connect attempt accepted`)
            }
            else {
                //auth rejected
                connObj.end()
                connObj.destroy()
                self.logger.debug(`ClientId ${packet.clientId} auth failed, disconnect client`)
            }
        }.bind(self, connObj, packet))

    }

    _onPublishHandler(connObj, packet) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt !== undefined) {
            self.logger.silly(`Published message from ${conPkt.clientId} with topic ${packet.topic}, length : ${packet.payload.length}`)
            //call user provided handle
            self.clientPublish(packet, conPkt)
            connObj[PACKET_TIMESTAMP] = Date.now()
        }
        else {
            //ignore message from client not on the list, maybe client on process of disconnecting
            self.logger.warn(`Receive publish attempt from conenction without clientId`)
        }
    }

    _onSubscribeHandler(connObj, packet) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt !== undefined) {
            self.logger.debug(`Receive subscribe request from ${conPkt.clientId} with topic ${packet.subscriptions[0].topic}`)
            //call user provided handle
            self.clientSubscribe(packet.subscriptions[0].topic, conPkt, function (conn, conPkt, packet, res) {
                if (res) {
                    //approve subscription, only granted one topic with qos 0
                    conn.suback({ granted: [0], messageId: packet.messageId })
                    self.logger.info(`Subscribe request from ${conPkt.clientId} approved`)
                }
                else {
                    self.logger.info(`Subscribe request from ${conPkt.clientId} rejected`)
                }
            }.bind(self, connObj, conPkt, packet))
        }
        else {
            self.logger.warn(`Receive subscribe attempt from conenction without clientId`)
            //ignore message from client not on the list, maybe client on process of disconnecting
        }

    }

    _onUnsubscribeHandler(connObj, packet) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt !== undefined) {
            self.logger.debug(`Receive unsubscribe request from ${conPkt.clientId} with topic ${packet.unsubscriptions[0]}`)
            self.clientUnsubscribe(packet.unsubscriptions[0], conPkt)
            connObj.unsuback({messageId: packet.messageId })
        }
    }

    _onDisconnectHandler(connObj, packet) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt !== undefined) {
            self.logger.debug(`Receive disconnect packet from ${conPkt.clientId}`)
            connObj[DISCONNECT_PACKET] = packet
        }
        else {
            self.logger.warn(`Receive disconnect packet from connection without clientId`)
        }
        connObj.end()
        connObj.destroy()
    }

    _onErrorHandler(connObj) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        connObj.destroy()
        if (conPkt === undefined) {
            self.logger.warn("Connection error, unknown cause")
        }
        else {
            self.logger.warn(`Connection related to ${conPkt.clientId} error, unknown cause`)
            //delete self.clientList[conPkt.clientId]
            self.clientMap.delete(conPkt.clientId)
        }
    }

    _onCloseHandler(connObj, how) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt === undefined) {
            self.logger.warn(`Connection without associated clientId has been disconnected`)
            return
        }
        self.clientDisconnect(conPkt)
        if (connObj[DISCONNECT_PACKET] === undefined) {
            self.logger.warn(`ClientId ${conPkt.clientId} connection has been closed without sending disconnect packet`)
            //delete self.clientList[conPkt.clientId]
            self.clientMap.delete(conPkt.clientId)
            return
        }
        else {
            self.logger.info(`ClientId ${conPkt.clientId} connection has been closed`)
            //delete self.clientList[conPkt.clientId]
            self.clientMap.delete(conPkt.clientId)
            return;
        }
    }

    _onPingHandler(connObj) {
        var self = this
        var conPkt = connObj[CONNECT_PACKET]
        if (conPkt === undefined) {
            self.logger.warn(`Ping from no one`)
            return
        }
        self.logger.info(`Ping from ${conPkt.clientId}`)
        connObj[PACKET_TIMESTAMP] = Date.now()
        connObj.pingresp()
    }

    _handler(stream) {
        var self = this
        self.logger.info("New socket connection")
        var clientConn = mqttCon(stream)
        var evictCanceller = self._evictConnection(clientConn, self.timeout)
        clientConn.on('connect', self._onConnectHandler.bind(self, clientConn))
        clientConn.on('connect', clearInterval.bind(null, evictCanceller))

        clientConn.on('publish', self._onPublishHandler.bind(self, clientConn))

        clientConn.on('pingreq', self._onPingHandler.bind(self, clientConn));

        clientConn.on('subscribe', self._onSubscribeHandler.bind(self, clientConn))

        clientConn.on('unsubscribe', self._onUnsubscribeHandler.bind(self, clientConn))

        clientConn.on("disconnect", self._onDisconnectHandler.bind(self, clientConn))

        clientConn.on("error", self._onErrorHandler.bind(self, clientConn))
        clientConn.on("close", self._onCloseHandler.bind(self, clientConn))
    }
}

module.exports = umqtt 