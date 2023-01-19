import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry'

const registry = new SchemaRegistry({ host: 'https://psrc-k0w8v.us-central1.gcp.confluent.cloud',   auth: {
  username: `${process.env.SCHEMA_USERNAME}`,
  password: `${process.env.SCHEMA_PASSWORD}`,
}, })


const schema = `
{
  "connect.name": "io.confluent.ksql.avro_schemas.KsqlDataSourceSchema",
  "fields": [
    {
      "default": null,
      "name": "CHARACTER_ID",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "LINECOUNT",
      "type": [
        "null",
        "string"
      ]
    }
  ],
  "name": "KsqlDataSourceSchema",
  "namespace": "io.confluent.ksql.avro_schemas",
  "type": "record"
}`

const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema
})


const charText_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/charText/").then(
    (response) => response.text()
  );
};

const synopsis_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/synopsis/").then(
    (response) => response.text()
  );
};

const monologue_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/monologue/").then(
    (response) => response.text()
  );
};

const syncedSynopsisMsg = synopsis_message().then((e) => e);

const syncedMonologueMsg = monologue_message().then((e) => {
  let eachLine = e.split("\n");

  let arrayOfKeysAndValues = [];

  for (let i = 0; i < eachLine.length; i++) {
    let key = "initialKeyFromClient";
    let value = "initialValueFromClient";

    const regex = /(\([^]*)/;
    const numRegex = /(\)[^]*)/gs;
    const nameRegex = /[a-zA-Z \(]+/;

    let cutNameOut = eachLine[i].replace(regex, "");

    key = cutNameOut.replace(" ", "");

    let divCutOut = eachLine[i].replace(numRegex, "");

    value = divCutOut.replace(nameRegex, "");

    if (key !== "") {
      arrayOfKeysAndValues.push({ key, value });
    }
  }

  return arrayOfKeysAndValues;
});

const syncedCharTextMsg = charText_message().then((e) => {
  let eachLine = e.split("\n");

  let arrayOfKeysAndValues = [];

  for (let i = 0; i < eachLine.length; i++) {
    let key = "initialKeyFromClient";
    let value = "initialValueFromClient";

    const notANumRegex = /[^0-9]/g;
    const inADivRegex = /<([^]*)>/gs;

    let cutNumberOut = eachLine[i].replace(notANumRegex, "");

    value = cutNumberOut;

    let cutLastHTMLElementsOut = eachLine[i].replace("</a></div><br/>", "");

    let getNameByRegex = cutLastHTMLElementsOut.replace(inADivRegex, "");
    let removeSlashR = getNameByRegex.replace("\r", "");

    key = removeSlashR;

    if (key !== "" && key !== "\r") arrayOfKeysAndValues.push({ key, value });
  }

  return arrayOfKeysAndValues;
});

const kafka = new Kafka({
  clientId: "my-app",
  brokers: [process.env.BOOTSTRAP],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KEY,
    password: process.env.SECRET,
  },
});

const producer = kafka.producer();

const run = async () => {
  const arrayOfCharTextMessages = await syncedCharTextMsg;
  const synopsis = await syncedSynopsisMsg;
  const monologue = await syncedMonologueMsg;


  for (let i = 0; i < arrayOfCharTextMessages.length; i++) {
    await producer.connect();
    //will need to correct topic titles soon
    let buff = Buffer.from(JSON.stringify({ character_id : arrayOfCharTextMessages[i].key, linecount: arrayOfCharTextMessages[i].value}));

    const encodedPayload = await registry.encode(id, buff)
   
    await producer.send({
      topic: "charText_MND",
      messages: [
        {
          key: arrayOfCharTextMessages[i].key,
          value: encodedPayload
        }
      ],
    });
  }

  for (let i = 0; i < monologue.length; i++) {
    await producer.connect();

    let buff = Buffer.from(JSON.stringify({ character_id : monologue[i].key, linecount: monologue[i].value}));
    const encodedPayload = await registry.encode(id, buff)

    await producer.send({
      topic: "monologue_MND",
      messages: [{ key: monologue[i].key, value: encodedPayload}],
    });

    console.log( [{ key: monologue[i].key, value: encodedPayload}])
  }

  await producer.send({
    topic: "synopsis",
    messages: [{ key: "Midsummer Night's Dream", value: synopsis }],
  });

  await producer.disconnect();
};

run().catch((e) =>
  kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack })
);