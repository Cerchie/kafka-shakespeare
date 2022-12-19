import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

// console.table([['server', process.env.BOOTSTRAP],['key',process.env.KEY
// ],['secret', process.env.SECRET]])

const message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/charText/")
    .then((response) => response.text()
    )
};

const charText_message = () => {
  return fetch("https://www.folgerdigitaltexts.org/MND/concordance/")
    .then((response) => response.text())
};

 const syncedCharTextMsg = message().then((e) => {
  let eachLine = e.split("\n");
  for (let i = 0; i < eachLine.length; i++){
    let key
    let value
//NEED TO FIGURE OUT HOW TO PRODUCE THESE AS SEPARATE EVTS

    console.log('I EVENT', eachLine[i])
  }
  return e
})

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
  await producer.connect();
  await producer.send({
    topic: "test-topic",
    messages: [{ value: await syncedMsg}],
  });

  await producer.disconnect();
};

run().catch((e) =>
  kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack })
);
