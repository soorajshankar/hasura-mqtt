var SparkplugClient = require("sparkplug-client");

// CONFIG
require("dotenv").config();
const MQTT_HOST = process.env.MQTT_HOST || "http://127.0.0.1:1883";
const DEVICE_COUNT = process.env.DEVICE_COUNT || 10;
const FREQUENCY = process.env.FREQUENCY || 200;

// assets
const Assets = [];
for (let i = 0; i <= DEVICE_COUNT; i++) {
  Assets.push({ UniqueID: i });
}
let clients = [];

var config = {
  //serverUrl: "tcp://52.172.44.240:1883",
  serverUrl: MQTT_HOST,
  username: "admin",
  password: "changeme",
  groupId: "Hasura Devices",
  edgeNode: "Hasura Edge Node",
  clientId: "HasuraSimpleEdgeNode",
  version: "spBv1.0",
};
hwVersion = "Emulated Hardware";
swVersion = "v1.0.0";
deviceId = "Emulated Device ";

randomInt = function () {
  return 1 + Math.floor(Math.random() * 10);
};

randomInt1 = function (START, END) {
  return START + Math.floor(Math.random() * END);
};

let getDeviceBirthPayload = function () {
  return {
    timestamp: new Date().getTime(),
    metrics: [
      { name: "my_boolean", value: Math.random() > 0.5, type: "boolean" },
      {
        name: "my_double",
        value: Math.random() * 0.123456789,
        type: "double",
      },
      { name: "my_float", value: Math.random() * 0.123, type: "float" },
      { name: "my_int", value: randomInt(), type: "int" },
      { name: "my_long", value: randomInt() * 214748364700, type: "long" },
      { name: "Inputs/0", value: true, type: "boolean" },
      { name: "Inputs/1", value: 0, type: "int" },
      { name: "Inputs/2", value: 1.23, type: "float" },
      { name: "Outputs/0", value: true, type: "boolean" },
      { name: "Outputs/1", value: 0, type: "int" },
      { name: "Outputs/2", value: 1.23, type: "float" },
      { name: "Properties/hw_version", value: hwVersion, type: "string" },
      { name: "Properties/sw_version", value: swVersion, type: "string" },
      {
        name: "my_dataset",
        type: "dataset",
        value: {
          numOfColumns: 2,
          types: ["string", "string"],
          columns: ["str1", "str2"],
          rows: [
            ["x", "a"],
            ["y", "b"],
          ],
        },
      },
      {
        name: "TemplateInstance1",
        type: "template",
        value: {
          templateRef: "Template1",
          isDefinition: false,
          metrics: [
            { name: "myBool", value: true, type: "boolean" },
            { name: "myInt", value: 100, type: "int" },
          ],
          parameters: [
            {
              name: "param1",
              type: "string",
              value: "value2",
            },
          ],
        },
      },
    ],
  };
};

// function devicebirth() {

Assets.map((i) => {
  config.clientId = i.UniqueID;
  let new_client = SparkplugClient.newClient(config);

  new_client.clientId = i.UniqueID;
  clients.push(new_client);

  // Publish Device BIRTH certificate
  console.log("publishing BIRTH");
  new_client.publishDeviceBirth(i.UniqueID, getDeviceBirthPayload());
  
  setInterval(() => {
    // console.log("publishing DATA");
    Assets.forEach((i, ix) => {
      publish(ix);
    });
  }, FREQUENCY);// poll all devices in every 200ms
});

function publish(i) {
  console.log("new client length", clients.length);
  if (clients.length > 0) {
    var randomNumber = i || randomInt1(0, clients.length - 1);
    var client = clients[randomNumber];

    myDeviceid = deviceId + client.clientId;
    client.publishDeviceData(myDeviceid, getDataPayload());
    console.log(myDeviceid);
  }
}
setInterval(() => {
  console.log("Tick");
}, 1 << 30);

function getDataPayload() {
  let payload = [
    {
      version: "v0.6.0",
      time_stamp: new Date(),
      last_query_time: "2019-04-01 06:47:05",
    },
  ];

  return {
    timestamp: new Date().getTime(),
    metrics: [
      {
        name: "Device_Payload",
        value: JSON.stringify(payload),
        type: "string",
      },
    ],
  };
}
