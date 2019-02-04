'use strict';

const Hapi = require('hapi');
const _ = require('lodash')
var corsHeaders = require('hapi-cors-headers')

var fail_counter = 0


const server = Hapi.server({
    port: 10000,
    host: 'localhost',
    routes: {
        cors: true
    }
});

//server.ext('onPreResponse', corsHeaders)

var topicMap = {
    all: "tenant1.clientPublish.*.gps",
    groupa: "analytic.pipeline1.groupa",
    titik0: "analytic.pipeline2.titik0",
    distance: "analytic.pipeline3.distance"
}

var description = [
    {topic:"all",desc : "semua asset"},
    {topic:"groupa",desc : "asset di group a"},
    {topic:"titik0",desc : "asset yang berada 2000m dari km0"},
    {topic:"distance",desc : "akumulasi jarak semua aset"}
]

var topicDesc = _.keyBy(description,(x)=>x.topic)

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  

server.route({
    method: 'GET',
    path: '/topicList',
    handler: async (request, h) => {
        await delay(1000)
        let topics = [{ topic: "all", type: "map" }, { topic: "titik0", type: "map" }, { topic: "groupa", type: "map" }, { topic: "distance", type: "chart" }]
        return _.keyBy(topics,(x) => x.topic);
    }
});

server.route({
    method: 'GET',
    path: '/topicDescription',
    handler :async (request,h)=>{
        await delay(1000)
        return _.pick(topicDesc,request.query.topic)
    }
})

server.route({
    method: 'GET',
    path: '/mapTopic/{topic}',
    handler: (request, h) => {
        return topicMap[request.params.topic];
    }
});

const init = async () => {

    await server.start();
    console.log(`Server running at: ${server.info.uri}`);
};

process.on('unhandledRejection', (err) => {

    console.log(err);
    process.exit(1);
});

init();
