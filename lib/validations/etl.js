"use strict";
const Promise = require("bluebird");
const config = require("../../config/import.config");
const request = require("request-promise");
const WebSocket = require("ws");
const Logger = require("../logger");
const log = new Logger({ scope: "validations etl" });
const colors = require("colors");
const PEER_PORT_REGEX = /51235/g;
const WS_PORT = "51233";
const manifests = require("./manifests")(config.get("hbase"));
const validations = require("./validations")(
  config.get("hbase"),
  config.get("validator-config")
);
const CronJob = require("cron").CronJob;
var moment = require('moment');

const connections = {};

/**
 * requestSubscribe
 */

function requestSubscribe(ws, altnet) {
  if (altnet) {
    ws.send(
      JSON.stringify({
        id: 222,
        command: "server_info"
      })
    );
  }

  ws.send(
    JSON.stringify({
      id: 1,
      command: "subscribe",
      streams: ["validations"]
    })
  );

  ws.send(
    JSON.stringify({
      id: 2,
      command: "subscribe",
      streams: ["manifests"]
    })
  );
}

/**
 * subscribe
 */

function subscribe(rippled) {
  const ip =
    (rippled.altnet ? "wss://" : "ws://") +
    rippled.ipp.replace(PEER_PORT_REGEX, WS_PORT);

  // resubscribe to open connections
  if (connections[ip] && connections[ip].readyState === WebSocket.OPEN) {
    try {
      requestSubscribe(connections[ip], rippled.altnet);
      return;
    } catch (e) {
      log.error(e.toString().red, ip.cyan);
      delete connections[ip];
    }
  } else if (connections[ip]) {
    connections[ip].close();
    delete connections[ip];
  }

  const ws = new WebSocket(ip);

  connections[ip] = ws;

  ws.public_key = rippled.public_key;

  // handle error
  ws.on("close", function() {
    log.info(this.url.cyan, "closed".yellow);
    if (this.url && connections[this.url]) {
      delete connections[this.url];
    }
  });

  // handle error
  ws.on("error", function(e) {
    if (this.url && connections[this.url]) {
      this.close();
      delete connections[this.url];
    }
  });

  // subscribe and save new connections
  ws.on("open", function() {
    if (this.url && connections[this.url]) {
      requestSubscribe(this, rippled.altnet);
    }
  });

  // handle messages
  ws.on("message", function(message) {
    const data = JSON.parse(message);

    if (data.type === "validationReceived") {
      data.reporter_public_key = connections[this.url].public_key;

      // Store master key if validation is signed
      // by a known valid ephemeral key
      const master_public_key = manifests.getMasterKey(
        data.validation_public_key
      );
      if (master_public_key) {
        data.ephemeral_public_key = data.validation_public_key;
        data.validation_public_key = master_public_key;
      }

      validations.handleValidation(data).catch(e => {
        log.error(e);
      });
    } else if (data.type === "manifestReceived") {
      manifests.handleManifest(data);
    } else if (data.error === "unknownStream") {
      delete connections[this.url];
      log.error(data.error, this.url.cyan);
    } else if (data.id === 222) {
      connections[this.url].public_key = data.result.info.pubkey_node;
    }
  });
}

/**
 * getRippleds
 */

function getRippleds() {
  var url = config.get("rippleds_url");

  log.info({
    url: url,
    json: true
  });
  if (!url) {
    return Promise.reject("requires rippleds url");
  }

  return request
    .get({
      url: url,
      json: true
    })
    .then(d => {
      return d.nodes;
    });
}

/**
 * subscribeToRippleds
 */

function subscribeToRippleds(rippleds) {
  const nRippled = rippleds.length.toString();
  const nConnections = Object.keys(connections).length.toString();

  log.info(("rippleds: " + nRippled).yellow);
  log.info(("connections: " + nConnections).yellow);

  // Subscribe to validation websocket subscriptions from rippleds
  rippleds.forEach(rippled => {
    if (rippled.ip && rippled.port) {
      subscribe({
        ipp: rippled.ip + ":" + rippled.port,
        public_key: rippled.node_public_key
      });
    }
  });

  // subscribe({
  //   ipp: 's.altnet.rippletest.net:51235',
  //   public_key: 'altnet',
  //   altnet: true
  // });

  return connections;
}

/**
 * start
 */

function refreshSubscriptions() {
  getRippleds()
    .then(subscribeToRippleds)
    .catch(e => {
      log.error(e.toString().red);
    });
}

function importManifests() {
  log.info("Importing...");
  var url = "https://data.ripple.com/v2/network/validators"; //config.get("rippleds_url");
  var url2 = "https://data.ripple.com/v2/network/validators/#/manifests";

  if (!url) {
    return Promise.reject("requires validators url");
  }
  let count = 0;
  return request
    .get({
      url: url,
      json: true
    })
    .then(d => {
      console.log("validators> ", d.validators.length);
      
      const promises = [];
      let i = 0;

      const cutoff = moment().add(-30, 'd');
      console.log("comapring with " + cutoff);
      const lastValidators = d.validators.filter(v => moment(v.last_datetime).isAfter(cutoff));

      console.log("#", lastValidators.length);
      const clear = setInterval(() => {
        if(i >= lastValidators.length){
          clearInterval(clear);
          return;
        }
        const v = lastValidators[i++];
        const promise = request.get({
          url: url2.replace("#", v.validation_public_key),
          json: true
        }).then(m => {
          log.info(v.domain || "unverified> ", v.validation_public_key);
          if(v.domain){
            console.log("manifests for ", v.domain, " ", m.manifests.length);
          }
          if(m && m.manifests && m.manifests.length > 0){
            const lastManifestData = m.manifests[0];
            const manifest = {
              master_key: lastManifestData.master_public_key, // -> master_public_key
              signing_key: lastManifestData.ephemeral_public_key, // -> ephemeral_public_key
              seq: lastManifestData.sequence, // -> sequence
              signature: lastManifestData.signature, // -> signature
              master_signature: lastManifestData.master_signature // -> master_signature
            };
            return manifests.handleManifest(manifest);
          } else {
            return Promise.resolve();
          }
        });
        promises.push(promise);
      }, 4 * 1000);

      return Promise.all(promises);
    }).then(() => {
      console.log("done", count);      
    }).catch((err) => {
      console.log("failed");
      return Promise.resolve("OK");
    });
}

importManifests();

manifests.start().then(() => {
  // refresh connections
  // every minute
  setInterval(refreshSubscriptions, 60 * 1000);
  refreshSubscriptions();
  validations.start();
  // validations.verifyDomains();
});
