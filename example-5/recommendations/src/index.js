const express = require("express");
const mongodb = require("mongodb");
const amqp = require("amqplib");

// Environment variable validations
if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}
if (!process.env.DBHOST) {
    throw new Error("Please specify the database host using environment variable DBHOST.");
}
if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME.");
}
if (!process.env.RABBIT) {
    throw new Error("Please specify the RabbitMQ host using environment variable RABBIT.");
}

const PORT = process.env.PORT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
const RABBIT = process.env.RABBIT;

async function main() {
    const app = express();
    app.use(express.json()); // Enable JSON body parsing

    console.log("Connecting to the database...");
    const client = await mongodb.MongoClient.connect(DBHOST, { useNewUrlParser: true, useUnifiedTopology: true });
    const db = client.db(DBNAME);
    const videosCollection = db.collection("videos");

    console.log("Connecting to RabbitMQ...");
    const messagingConnection = await amqp.connect(RABBIT);
    const messageChannel = await messagingConnection.createChannel();

    await messageChannel.assertExchange("viewed", "fanout");

    const { queue } = await messageChannel.assertQueue("", { exclusive: true });
    console.log(`Created queue '${queue}', binding it to 'viewed' exchange.`);
    await messageChannel.bindQueue(queue, "viewed", "");

    async function consumeViewedMessage(msg) {
        try {
            // Log the raw message content to inspect it
            console.log("Received raw message:", msg.content.toString());

            const parsedMsg = JSON.parse(msg.content.toString());
            console.log("Parsed message:", parsedMsg);  // Log the parsed message

            if (parsedMsg && parsedMsg.videoId) {
                // Log the recommendation action
                console.log("Recommendation for video:", parsedMsg.videoId);
                console.log(`Recommendation video ${parsedMsg.videoId}`);

                // Store the view information in the database
                await videosCollection.insertOne({
                    videoId: parsedMsg.videoId,
                    viewedAt: new Date()
                });
                console.log("Message processed and stored in the database.");
            } else {
                console.warn("No videoId found in the message!");
            }

            messageChannel.ack(msg); // Acknowledge the message
        } catch (error) {
            console.error("Error processing message:", error);
        }
    }

    console.log("Starting to consume messages...");
    await messageChannel.consume(queue, consumeViewedMessage);

    app.get("/health", (req, res) => {
        res.json({ status: "OK" });
    });

    app.listen(PORT, () => {
        console.log(`Recommendation service is online on port ${PORT}.`);
    });
}

main().catch(err => {
    console.error("Microservice failed to start.", err);
});
