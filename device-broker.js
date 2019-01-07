'use strict'
const yaml = require('js-yaml');
const fs = require('fs');
const ymlPath = 'device-broker.yml'

//Load configuration file
const config = function () {
    try {
        const config = yaml.safeLoad(fs.readFileSync(ymlPath, 'utf8'));
        const indentedJson = JSON.stringify(config, null, 4);
        console.log("Your config file is as follow : ")
        console.log(indentedJson);
        return config
    }
    catch{
        console.error(`${ymlPath} not found`);
        process.exit()
    }
}.call()

var winston = require('winston')

const logger = winston.createLogger({
    level: config.logging.level,
    format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
    transports: [
        new winston.transports.File({ filename: config.logging.output })
    ]
});

logger.add(new winston.transports.Console());

//Setup conenction to NATS server
var nats = require('nats')
var nc = function () {
    try {
        return nats.connect(`nats://${config.nats.host}:${config.nats.port}`)
    }
    catch (e) {
        logger.error("NATS connect failed")
        process.exit()
    }
}.call()

nc.on("error", function () {
    logger.error("NATS connection error")
    process.exit()
})

nc.on('connect', function(nc) {
	logger.info("NATS connected")
});

nc.on('disconnect', function() {
	logger.warn("NATS disconnected")
});

nc.on('reconnecting', function() {
	logger.info("NATS reconnecting")
});

nc.on('reconnect', function(nc) {
	logger.info("NATS reconnected")
});

nc.on('close', function() {
	logger.info("NATS connection closed")
});

//setup mqtt server
var umqtt = require('./umqtt')
var mqtt = new umqtt({
    port: config.mqtt.port,
    protocol: config.mqtt.protocol,
    host: config.mqtt.host,
    logger: logger
})

mqtt.clientPublish = function (packet, client) {
    const topic = packet.topic

    let data = {
        topic: topic, payload: JSON.parse(packet.payload.toString()),
        clientId: client.clientId, arrivalTimestamp: (new Date()).getTime()
    }
    if (packet.qos != 0) {
        data.qos = packet.qos
        data.messageId = packet.messageId
    }
    if (packet.dup) { data.dup = true }
    nc.publish(`${config.nats.rootTopic}.clientPublish.${client.clientId}.${topic}`,
        JSON.stringify(data)
    )
}

mqtt.setup(function () {
    mqtt.run()
    nc.subscribe(`${config.nats.rootTopic}.serverPublish.*.*`, function (msg, reply, subj) {
        let topics = subj.split(".")
        let mqtttopic = topics[3] === undefined ? '' : topics[3]
        let clientId = topics[2]
        logger.info(`Publishing to clientId ${clientId} with topic ${mqtttopic}`)
        mqtt.publish(clientId, { topic: mqtttopic, payload: msg })
    })

    nc.subscribe(`${config.nats.rootTopic}.serverPubAck.*.*`, function (msg, reply, subj) {
        let topics = subj.split(".")
        let msgId = parseInt(topics[3])
        msgId = msgId === NaN ? 0 : msgId;
        let clientId = topics[2]
        logger.info(`PubAck to clientId ${clientId} with messageId ${msgId}`)
        mqtt.puback(clientId, msgId)
    })
})

//setup management api
var http = require('http')
var url = require('url')

var mgmtapi = http.createServer(function (req, res) {
    const urlobject = url.parse(req.url, true)
    if (req.method == 'GET' && urlobject.pathname === '/clients') {
        res.writeHead(200, {
            'Content-Type': 'application/json'
        });
        res.end(JSON.stringify(mqtt.getClientList()))
    }
    else if (req.method == 'GET' && urlobject.pathname === '/disconnect') {
        let clientId = urlobject.query.clientId
        if (clientId === undefined) {
            res.writeHead(404)
            res.end()
        }
        else {
            const stat = mqtt.disconnect(clientId)
            res.end(stat.toString())
        }
    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen(config.api.port)