const express = require("express");
const mongodb = require("mongodb");
const amqp = require("amqplib");

if (!process.env.PORT || !process.env.DBHOST || !process.env.DBNAME || !process.env.RABBIT) {
    throw new Error("Please specify the necessary environment variables.");
}

const PORT = process.env.PORT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
const RABBIT = process.env.RABBIT;

async function main() {
    const app = express();
    app.use(express.json());

    // Connect to MongoDB
    console.log("Connecting to the database...");
    const client = await mongodb.MongoClient.connect(DBHOST, { useNewUrlParser: true, useUnifiedTopology: true });
    const db = client.db(DBNAME);
    const videosCollection = db.collection("videos");

    // Connect to RabbitMQ
    console.log("Connecting to RabbitMQ...");
    const messagingConnection = await amqp.connect(RABBIT);
    const messageChannel = await messagingConnection.createChannel();

    await messageChannel.assertExchange("viewed", "fanout");

    // Create a temporary queue to receive messages from the 'viewed' exchange
    const { queue } = await messageChannel.assertQueue("", { exclusive: true });
    console.log(`Created queue '${queue}', binding it to 'viewed' exchange.`);
    await messageChannel.bindQueue(queue, "viewed", "");

    async function consumeViewedMessage(msg) {
        try {
            const parsedMsg = JSON.parse(msg.content.toString());
            console.log("Received a 'viewed' message:", JSON.stringify(parsedMsg, null, 4));

            // Process the message and store in the database
            if (parsedMsg.videoId) {
                await videosCollection.insertOne({
                    videoId: parsedMsg.videoId,
                    viewedAt: new Date()
                });
                console.log("Message processed and stored in the database.");
            } else {
                console.warn("Invalid message format: 'videoId' missing.");
            }

            messageChannel.ack(msg); // Acknowledge the message
        } catch (error) {
            console.error("Error processing message:", error);
        }
    }

    // Start consuming messages
    console.log("Starting to consume messages...");
    await messageChannel.consume(queue, consumeViewedMessage);

    app.get("/health", (req, res) => {
        res.json({ status: "OK" });
    });

    app.listen(PORT, () => {
        console.log(`History microservice is online on port ${PORT}.`);
    });
}

main().catch(err => {
    console.error("Microservice failed to start.", err);
});
