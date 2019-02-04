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
    let natsTopic = config.topic_mapping[topic]
    if (natsTopic == undefined) {
        logger.error("Topic not in mapping")
        done(false)
        return
    }
    logger.info(`topic ${topic} mapped to ${natsTopic}`)
    if (!subscriptionList.has(topic)) {
        let topic_member = { subscriber: new Set() }
        topic_member.subscriber.add(client.clientId)
        topic_member.sid = nc.subscribe(`${natsTopic}`,
            onMessage.bind(this, topic_member.subscriber, topic))
        subscriptionList.set(topic,topic_member)
    }
    else {
        subscriptionList.get(topic).subscriber.add(client.clientId)
    }
    done(true)
}

mqtt.clientUnsubscribe = function (topic, client) {
    if (subscriptionList.has(topic)) {
        let topic_member = subscriptionList.get(topic)
        topic_member.subscriber.delete(client.clientId)
        logger.info(`delete ${client.clientId} from ${topic}`)
        if (topic_member.subscriber.size === 0) {
            nc.unsubscribe(topic_member.sid)
            subscriptionList.delete(topic)
            logger.info(`delete ${topic} from subscription list`)
        }
    }
}

mqtt.clientDisconnect = function (client) {
    for (let [topic,topic_member] of subscriptionList) {
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
        for (let [key,value] of subscriptionList){
            subs[key] = [...value.subscriber]
        }
        res.end(JSON.stringify(subs))
    }
    else {
        res.writeHead(404)
        res.end()
    }
})

mgmtapi.listen(config.api.port)