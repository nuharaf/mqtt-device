'use strict'
const yaml = require('js-yaml');
const path = require('path');
const fs = require('fs');
const ymlPath = 'device-broker.yml'


var scriptname = path.basename(process.argv[1])
console.log(`Script name : ${scriptname}`)

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

//verify config
const Joi = require('joi');
const schema = Joi.object().keys({
    mqtt: Joi.object().keys({
        protocol: Joi.string().required().valid("tcp", "tls", "ws", "wss"),
        host: Joi.string().required(),
        port: Joi.number().port().required(),
        sameId: Joi.string().required().valid("prevent", "disconnect")
    }).required(),
    nats: Joi.object().keys({
        host: Joi.string().required(),
        port: Joi.number().port().required(),
        rootTopic: Joi.string().alphanum().required(),
    }).required(),
    acl: Joi.object().keys({
        connect: Joi.string(),
        publish: Joi.string()
    }).required(),
    logging: Joi.object().keys({
        level: Joi.string().required().valid("error", "warn", "info", "verbose", "debug", "silly"),
        output: Joi.string().required()
    }).required(),
    api: Joi.object().keys({
        port: Joi.number().port().required()
    })
})

Joi.validate(config, schema, function (err, value) {
    if (err) {
        console.log("Invalid config")
        console.log(err)
        process.exit()
    }
})

//Setup logging
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
        return nats.connect({
            url: `nats://${config.nats.host}:${config.nats.port}`,
            maxReconnectAttempts: -1,
            name: scriptname
        })
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

nc.on('connect', function (nc) {
    logger.info("NATS connected")
});

nc.on('disconnect', function () {
    logger.warn("NATS disconnected")
});

nc.on('reconnecting', function () {
    logger.info("NATS reconnecting")
});

nc.on('reconnect', function (nc) {
    logger.info("NATS reconnected")
});

nc.on('close', function () {
    logger.info("NATS connection closed")
});

//setup mqtt server
var umqtt = require('./umqtt')
var mqtt = new umqtt({
    port: config.mqtt.port,
    protocol: config.mqtt.protocol,
    host: config.mqtt.host,
    sameId: config.mqtt.sameId,
    logger: logger
})

//set callback when client attempt to connect
mqtt.connectAuthenticate = function (regex, delimiter, client, done) {
    for (let segment of client.clientId.split(delimiter)) {
        if (!segment.match(regex)) {
            done(false)
            logger.error(`Invalid clientId format : ${client.clientId}`)
            return
        }
    }

    //if acl connect topic specified, call it
    if (config.acl.connect != undefined) {
        nc.requestOne(config.acl.connect, JSON.stringify(client), {}, 1000, function (response) {
            if (response instanceof nats.NatsError && response.code === nats.REQ_TIMEOUT) {
                done(false)
                return;
            }
            try {
                const res = JSON.parse(response)
                if (res.status == 'OK') {
                    done(true)
                }
                else {
                    done(false)
                }
            } catch (error) {
                done(false)
            }
            return;
        })
    }
    else {
        done(true)
        return
    }

}.bind(this, new RegExp('^[a-zA-Z0-9-_]+$'), ".")

//set callback when client publish message
var LRU = require("lru-cache")
aclPublishCache = new LRU(500)
mqtt.clientPublish = function (regex, delimiter, packet, client) {
    const topic = packet.topic
    for (let segment of topic.split(delimiter)) {
        if (!segment.match(regex)) {
            logger.error(`Invalid topic format : ${topic}`)
            return
        }
    }

    function forward() {
        let payload
        try {
            payload = JSON.parse(packet.payload.toString());
        } catch (error) {
            logger.error("Invalid JSON syntax")
        }
        let data = {
            topic: topic, payload: payload,
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

    const aclKey = `${client.clientId}+${packet.topic}`
    if (config.acl.publish != undefined) {
        //check cache
        if (aclPublishCache.get(aclKey) == undefined) {
            //invoke permission request check
            nc.requestOne(config.acl.publish, JSON.stringify({ clientId: client.clientId, topic: packet.topic }),
                {}, 1000, function (response) {
                    if (response instanceof nats.NatsError && response.code === nats.REQ_TIMEOUT) {
                        return;
                    }
                    const res = JSON.parse(response)
                    if (res.status == 'OK') {
                        aclPublishCache.set(aclKey, true)
                        forward()
                    }
                    else {
                        aclPublishCache.set(aclKey, false)
                    }
                    return;
                })
        }
        else {
            if (aclPublishCache.get(aclKey)) {
                forward()
            }
        }
    }

}.bind(this, new RegExp('^[a-zA-Z0-9-_]+$'), ".")

mqtt.clientDisconnect = function (client) {
    nc.publish(`${config.nats.rootTopic}.clientDisconnect.${client.clientId}`)
}

//run the mqttserver
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
    else if (req.method == 'GET' && urlobject.pathname === '/publish') {
        let clientId = urlobject.query.clientId
        let message = urlobject.query.message
        let topic = urlobject.query.topic
        if (message == undefined) {
            message = ""
        }
        if (clientId === undefined) {
            res.writeHead(404)
            res.end()
            return
        }
        else {
            const stat = mqtt.publish(clientId, { topic: topic, payload: message })
            res.end(stat.toString())
        }
    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen({ host: "127.0.0.1", port: config.api.port })