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

const synopsis_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/synopsis/").then(
    (response) => response.text()
  );
};

const syncedSynopsisMsg = synopsis_message().then((e)=> e)



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
  const arrayOfCharTextMessages = await syncedCharTextMsg
  const synopsis = await syncedSynopsisMsg

  for (let i = 0; i < arrayOfCharTextMessages.length; i++) {

    await producer.connect();
    //will need to correct topic titles soon
    await producer.send({
      topic: "test-topic",
      messages: [
        { key: arrayOfCharTextMessages[i].key, value: arrayOfCharTextMessages[i].value }, 
      ],
    });
  }

  await producer.send({
    topic: "test-topic",
    messages: [
      { key: "synopsis", value: synopsis }, 
    ],
  });
  
  await producer.disconnect();
};

run().catch((e) =>
  kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack })
);
