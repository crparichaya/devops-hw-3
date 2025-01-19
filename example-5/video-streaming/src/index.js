const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');

if (!process.env.PORT || !process.env.RABBIT) {
    throw new Error("Please specify the PORT and RABBIT environment variables.");
}

const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;

async function main() {
    console.log(`Connecting to RabbitMQ at ${RABBIT}...`);
    const connection = await amqp.connect(RABBIT);
    const channel = await connection.createChannel();

    await channel.assertExchange("viewed", "fanout");

    const app = express();

    app.get("/video", async (req, res) => {
        const videoId = req.query.id;

        const videoPaths = {
            "1": "videos/855081-hd_1920_1080_25fps (1).mp4",
            "2": "videos/3042473-uhd_3840_2160_30fps.mp4"
        };

        const videoPath = videoPaths[videoId];
        if (!videoPath) {
            return res.status(404).send("Video not found");
        }

        const stats = await fs.promises.stat(videoPath);
        res.writeHead(200, {
            "Content-Length": stats.size,
            "Content-Type": "video/mp4",
        });

        fs.createReadStream(videoPath).pipe(res);

        // Publish a message to the "viewed" exchange
        const message = { videoId, viewed: `viewed-${videoId}` };
        channel.publish("viewed", "", Buffer.from(JSON.stringify(message)));
    });

    app.listen(PORT, () => console.log(`Video service listening on port ${PORT}`));
}

main().catch(console.error);
