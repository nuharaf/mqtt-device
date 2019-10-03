'use strict'
const yaml = require('js-yaml');
const path = require('path');
const fs = require('fs');
const argv = require('yargs').argv

var scriptname = path.basename(process.argv[1])
console.log(`Script name : ${scriptname}`)
var ymlPath = 'device-broker.yml'
if(argv.config == undefined){
    console.log(`Using default config file : ${ymlPath}`)
}
else{
    ymlPath = argv.config
    console.log(`Using config file : ${ymlPath}`)
}

//Load configuration file
const rawConfig = function () {
    try {
        const config = yaml.safeLoad(fs.readFileSync(ymlPath, 'utf8'));
        return config
    }
    catch{
        console.error(`${ymlPath} not found`);
        process.exit()
    }
}.call()


//verify config
const Joi = require('@hapi/joi');
const schema = Joi.object().keys({
    mqtt: Joi.object().keys({
        protocol: Joi.string().required().valid("tcp", "tls", "ws", "wss"),
        host: Joi.string().ip().required(),
        port: Joi.number().port().required(),
        sameId: Joi.string().required().valid("prevent", "disconnect")
    }).required(),
    nats: Joi.object().keys({
        host: Joi.string().ip().required(),
        port: Joi.number().port().required(),
        publish_mapping: Joi.string().required(),
        timestamp: Joi.boolean().default(false),
        server_publish: Joi.string(),
        server_puback: Joi.string(),
    }).required(),
    service: Joi.object().keys({
        connect_authz: Joi.string(),
        publish_authz: Joi.string()
    }).empty(null).default({}),
    logging: Joi.object().keys({
        level: Joi.string().required().valid("error", "warn", "info", "verbose", "debug", "silly"),
        output: Joi.string()
    }).required(),
    api: Joi.object().keys({
        port: Joi.number().port().required(),
        host: Joi.string().ip().required()
    })
})
const {error,value} = schema.validate(rawConfig)
if(error){
    console.log(error.toString)
    process.exit()
}

var config = value;

const stringifyObject = require('stringify-object');
console.log(stringifyObject(config))
var publish_mapping = eval(config.nats.publish_mapping)

//Setup logging
var winston = require('winston')
const logger = winston.createLogger({
    level: config.logging.level,
    format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
    transports: []
});
if(config.logging.output != undefined){
    logger.add(new winston.transports.File({ filename: config.logging.output }));    
}
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
    //if  connect_authz service specified, call it
    if (config.service.connect_authz != undefined) {
        nc.requestOne(config.service.connect_authz, JSON.stringify(client), {}, 1000, function (response) {
            if (response instanceof nats.NatsError && response.code === nats.REQ_TIMEOUT) {
                done(false)
                return;
            }
            try {
                const res = JSON.parse(response)
                if (res.status == 'OK') {
                    done(true)
                    nc.publish(`${config.nats.rootTopic}.clientConnect`,client.clientId)
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
        nc.publish(`${config.nats.rootTopic}.clientConnect`,client.clientId)
        return
    }

}.bind(this, new RegExp('^[a-zA-Z0-9-_]+$'), "-")

//set callback when client publish message
var LRU = require("lru-cache")
var aclPublishCache = new LRU(500)
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
            clientId: client.clientId,
            username : client.username
        }
        if (config.nats.timestamp) {
            data.arrivalTimestamp = (new Date()).getTime()
        }
        if (packet.qos != 0) {
            data.qos = packet.qos
            data.messageId = packet.messageId
        }
        if (packet.dup) { data.dup = true }
        let nats_topic = publish_mapping(client.clientId, topic)
        if (nats_topic == "") { return }
        nc.publish(nats_topic, JSON.stringify(data))
    }

    const aclKey = `${client.clientId}+${packet.topic}`
    if (config.service.publish_authz != undefined) {
        //check cache
        if (aclPublishCache.get(aclKey) == undefined) {//cache miss
            //invoke permission check
            nc.requestOne(config.service.publish_authz, JSON.stringify({ clientId: client.clientId, topic: packet.topic }),
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
        else {//cache hit
            if (aclPublishCache.get(aclKey)) {
                forward()
            }
        }
    }
    else {
        forward()
    }

}.bind(this, new RegExp('^[a-zA-Z0-9-_]+$'), "-")

mqtt.clientDisconnect = function (client) {
    nc.publish(`${config.nats.rootTopic}.clientDisconnect`,client.clientId)
}

//run the mqttserver
mqtt.setup(function () {
    mqtt.run()
    nc.subscribe(`${config.nats.server_publish}`, function (msg, reply, subj) {
        let topics = subj.split(".")
        let mqtttopic = topics[3] === undefined ? '' : topics[3]
        let clientId = topics[2]
        logger.info(`Publishing to clientId ${clientId} with topic ${mqtttopic}`)
        mqtt.publish(clientId, { topic: mqtttopic, payload: msg })
    })

    nc.subscribe(`${config.nats.server_puback}`, function (msg, reply, subj) {
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
    else if (req.method == 'GET' && urlobject.pathname === '/cache') {
        let cache = {}
        aclPublishCache.forEach(function (value, key) {
            cache[key] = value
        })
        res.writeHead(200, {
            'Content-Type': 'application/json'
        });
        res.end(JSON.stringify(cache))

    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen({ host: config.api.host, port: config.api.port })
