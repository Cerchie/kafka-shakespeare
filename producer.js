import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

// console.table([['server', process.env.BOOTSTRAP],['key',process.env.KEY
// ],['secret', process.env.SECRET]])

const charText_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/charText/").then(
    (response) => response.text()
  );
};

const syncedCharTextMsg = charText_message().then((e) => {
  let eachLine = e.split("\n");

  let arrayOfKeysAndValues = [];

  for (let i = 0; i < eachLine.length; i++) {
    let key = "initialKey";
    let value = "initialValue";

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
  const arrayOfMessages = await syncedCharTextMsg
  for (let i = 0; i < arrayOfMessages.length; i++) {
console.log( { key: arrayOfMessages[i].key, value: arrayOfMessages[i].value })
    await producer.connect();
    await producer.send({
      topic: "test-topic",
      messages: [
        { key: arrayOfMessages[i].key, value: arrayOfMessages[i].value },
      ],
    });
  }
  await producer.disconnect();
};

run().catch((e) =>
  kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack })
);
