'use strict'
const yaml = require('js-yaml');
const path = require('path');
const fs = require('fs');
const ymlPath = 'web-broker.yml'
var async = require("async");

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
            name : scriptname
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

var subscriptionList = {}


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
    let topic_member = subscriptionList[topic]
    let natsTopic = config.topic_mapping[topic]
    if (natsTopic == undefined) {
        logger.error("Topic not in mapping")
        done(false)
        return
    }
    if (topic_member === undefined) {
        topic_member = { subscriber: new Set() }
        topic_member.subscriber.add(client.clientId)
        topic_member.sid = nc.subscribe(`${natsTopic}`,
            onMessage.bind(this, topic_member.subscriber, topic))
        subscriptionList.topic = topic_member
    }
    else {
        topic.subscriber.add(client.clientId)
    }
    done(true)
}

mqtt.clientUnsubscribe = function (topic, client) {
    let topic = subscriptionList.topic
    if (topic !== undefined) {
        topic.subscriber.delete(client.clientId)
        if (topic.subscriber.size === 0) {
            nc.unsubscribe(topic.sid)
            delete subscriptionList.topic
        }
    }
}

mqtt.clientDisconnect = function (client) {
    for (let topic in subscriptionList.topic) {
        topic.delete(client.clientId)
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
        res.end(JSON.stringify(subscriptionList))
    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen(config.api.port)