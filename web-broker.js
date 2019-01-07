'use strict'
const yaml = require('js-yaml');
const fs = require('fs');
const ymlPath = 'web-broker.yml'
var async = require("async");

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
    catch{
        console.error("Unable to connect to nats server")
        process.exit()
    }
}.call()

var subscriptionList = {}


//setup mqtt server
var umqtt = require('./umqtt')
var mqtt = new umqtt({
    port: config.mqtt.port,
    protocol: config.mqtt.protocol,
    host: config.mqtt.host,
    logger: logger
})

mqtt.clientSubscribe = function (topic, client, done) {
    let topic = subscriptionList.topic
    if (topic === undefined) {
        topic = { subscriber: new Set() }
        topic.subscriber.add(client.clientId)
        topic.sid = nc.subscribe(`${config.nats.rootTopic}.${topic}`, function (msg) {
            async.each(topic.subscriber, function (client, done) {
                const mqttmsg = {topic: topic, payload: msg }
                mqtt.publish(client, mqttmsg)
                done()
            })
        })
        subscriptionList.topic = topic
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