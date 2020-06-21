var mqtt = require("mqtt");
var fetch = require("node-fetch");
var sparkplug = require("sparkplug-payload").get("spBv1.0");
require("dotenv").config();
console.log(process.env.HASURA_HOST);

const MQTT_HOST = process.env.MQTT_HOST || "http://127.0.0.1:1883";
const HASURA_HOST = process.env.HASURA_HOST;

var client = mqtt.connect(MQTT_HOST);
client.on("connect", function () {
  console.log("CONNECTED");
  client.subscribe("spBv1.0/#", function (err) {
    if (!err) {
      console.log("SUBSCRIBTION STARTED");
      //       client.publish("spBv1.0/test", "Hello mqtt"); // test publish
    }
  });
});

client.on("message", function (topic, message) {
  // message is Buffer
  console.log(topic);
  const decoded = { message: decode(message), ...parseTopic(topic) };
  sendToHasura(decoded);
  //   setTimeout(() => {
  //     // client.publish("spBv1.0/safsa", "Hello mqtt");
  //   }, 1000);
  //   client.end() // to end the client
});

function decode(encoded) {
  return sparkplug.decodePayload(encoded);
}

function sendToHasura({
  message = "{test:'test'}",
  device_id = "testDevice",
  timestamp = new Date().toISOString(),
}) {
  fetch(`${HASURA_HOST}/v1/graphql`, {
    headers: {
      accept: "*/*",
      "accept-language": "en-US,en;q=0.9,ml;q=0.8",
      "content-type": "application/json",
      "X-Hasura-Role": "device",
      "X-Hasura-User-Id": device_id,
    },
    body: JSON.stringify({
      query:
        "mutation AddDeviceData($data: jsonb!, $device_id: String!, $timestamp: timestamptz!) {insert_health_data(objects: {data: $data, device_id: $device_id, timestamp: $timestamp}) {affected_rows}}",
      variables: {
        data: JSON.stringify(message),
        device_id,
        timestamp: message.timestamp || timestamp,
      },
      operationName: "AddDeviceData",
    }),
    method: "POST",
  })
    .then((res) => res.json())
    .then((resp) => console.log(JSON.stringify(resp, null, 2)));
}

function parseTopic(topic) {
  // namespace/group_id/message_type/edge_node_id/[device_id]
  if (topic.length && topic.split("/").length === 5) {
    const arr = topic.split("/");
    return {
      namespace: arr[0],
      group_id: arr[1],
      message_type: arr[2],
      edge_node_id: arr[3],
      device_id: arr[4],
    };
  }
  return {};
}
