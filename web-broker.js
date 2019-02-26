'use strict'
const yaml = require('js-yaml');
const path = require('path');
const fs = require('fs');
const ymlPath = 'web-broker.yml'
var async = require("async");

var scriptname = path.basename(process.argv[1])
console.log(`Script name : ${scriptname}`)

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
const Joi = require('joi');
const schema = Joi.object().keys({
    mqtt: Joi.object().keys({
        protocol: Joi.string().required().valid("tcp", "tls", "ws", "wss"),
        host: Joi.string().ip().required(),
        port: Joi.number().port().required()
    }).required(),
    nats: Joi.object().keys({
        host: Joi.string().ip().required(),
        port: Joi.number().port().required(),
        subscribe_mapping: Joi.string()
    }).required(),
    service: Joi.object().keys({
        connect_authz: Joi.string(),
        subscribe_authz: Joi.string(),
        topic_mapping: Joi.string()
    }).empty(null).default({}),
    logging: Joi.object().keys({
        level: Joi.string().required().valid("error", "warn", "info", "verbose", "debug", "silly"),
        output: Joi.string().required()
    }).required(),
    api: Joi.object().keys({
        port: Joi.number().port().required(),
        host: Joi.string().ip().required()
    }),

})

var config
Joi.validate(rawConfig, schema, function (err, value) {
    if (err) {
        console.log("Invalid config")
        console.log(err)
        process.exit()
    }
    config = value

})
const stringifyObject = require('stringify-object');
console.log(stringifyObject(config))
var subscribe_mapping = eval(config.nats.subscribe_mapping)

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
    catch{
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

var subscriptionList = new Map()


//setup mqtt server
var umqtt = require('./umqtt')
var mqtt = new umqtt({
    port: config.mqtt.port,
    protocol: config.mqtt.protocol,
    host: config.mqtt.host,
    logger: logger,
    sameId: "disconnect"
})

function onMessage(subscriber, topic, msg) {
    async.each(subscriber, function (client, done) {
        const mqttmsg = { topic: topic, payload: msg }
        mqtt.publish(client, mqttmsg)
        done()
    })
}

mqtt.clientSubscribe = function (topic, client, done) {
    let natsTopic = subscribe_mapping(client.clientId,topic)
    if(natsTopic == ""){
        done(false)
        return
    }
    logger.info(`topic ${topic} mapped to ${natsTopic}`)
    if (!subscriptionList.has(natsTopic)) {
        let topic_member = { subscriber: new Set() }
        topic_member.subscriber.add(client.clientId)
        topic_member.sid = nc.subscribe(`${natsTopic}`,
            onMessage.bind(this, topic_member.subscriber, topic))
        subscriptionList.set(natsTopic, topic_member)
    }
    else {
        subscriptionList.get(natsTopic).subscriber.add(client.clientId)
    }
    done(true)
}

mqtt.clientUnsubscribe = function (topic, client) {
    let natsTopic = subscribe_mapping(client.clientId,topic)
    if (subscriptionList.has(natsTopic)) {
        let topic_member = subscriptionList.get(natsTopic)
        topic_member.subscriber.delete(client.clientId)
        logger.info(`delete ${client.clientId} from ${natsTopic}`)
        if (topic_member.subscriber.size === 0) {
            nc.unsubscribe(topic_member.sid)
            subscriptionList.delete(natsTopic)
            logger.info(`delete ${natsTopic} from subscription list`)
        }
    }
}

//set callback when client attempt to connect
mqtt.connectAuthenticate = function (client, done) {
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
        //or else just approve the conenction request
        done(true)
        return
    }

}.bind(this)

mqtt.clientDisconnect = function (client) {
    for (let [topic, topic_member] of subscriptionList) {
        if (topic_member.subscriber.has(client.clientId)) {
            topic_member.subscriber.delete(client.clientId)
            if (topic_member.subscriber.size === 0) {
                nc.unsubscribe(topic_member.sid)
                subscriptionList.delete(topic)
            }
        }
    }
}

mqtt.setup(function () {
    mqtt.run()
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
    else if (req.method == 'GET' && urlobject.pathname === '/subscriptions') {
        res.writeHead(200, {
            'Content-Type': 'application/json'
        });
        var subs = {}
        for (let [key, value] of subscriptionList) {
            subs[key] = [...value.subscriber]
        }
        res.end(JSON.stringify(subs))
    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen({ host: config.api.host, port: config.api.port })