"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const logger_1 = require("./utils/logger");
const rabbitmq_1 = require("./services/rabbitmq");
const redis_1 = require("./services/redis");
const messageProcessor_1 = require("./services/messageProcessor");
async function startWorker() {
    try {
        // Connect to Redis
        const redisClient = await (0, redis_1.connectRedis)();
        logger_1.logger.info('Connected to Redis');
        // Connect to RabbitMQ and start consuming messages
        const { channel, connection, dlqExchange, dlqRoutingKey } = await (0, rabbitmq_1.connectRabbitMQ)();
        logger_1.logger.info('Connected to RabbitMQ');
        const queue = process.env.RABBITMQ_QUEUE;
        // Set up consumer
        channel.consume(queue, async (msg) => {
            if (msg) {
                try {
                    const content = msg.content.toString();
                    logger_1.logger.info(`Received message: ${content}`);
                    // Process the message
                    await (0, messageProcessor_1.processMessage)(JSON.parse(content), redisClient, channel, dlqExchange, dlqRoutingKey);
                    // Acknowledge the message
                    channel.ack(msg);
                }
                catch (error) {
                    const errorMessage = error instanceof Error ? error.message : String(error);
                    logger_1.logger.error(`Error processing message: ${errorMessage}`);
                    // Reject the message and don't requeue
                    channel.nack(msg, false, false);
                }
            }
        });
        logger_1.logger.info(`Worker started and consuming from queue: ${queue}`);
        // Handle graceful shutdown
        const shutdown = async () => {
            logger_1.logger.info('Shutting down worker...');
            await channel.close();
            // Close the connection
            if (connection) {
                // The connection is properly typed as ChannelModel which has the close method
                await connection.close();
            }
            await redisClient.quit();
            process.exit(0);
        };
        process.on('SIGINT', shutdown);
        process.on('SIGTERM', shutdown);
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.logger.error(`Worker startup failed: ${errorMessage}`);
        process.exit(1);
    }
}
startWorker();
//# sourceMappingURL=index.js.map