"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.connectRabbitMQ = connectRabbitMQ;
exports.sendToDLQ = sendToDLQ;
const amqplib_1 = __importDefault(require("amqplib"));
const logger_1 = require("../utils/logger");
async function connectRabbitMQ() {
    try {
        const url = process.env.RABBITMQ_URL;
        const queue = process.env.RABBITMQ_QUEUE;
        const exchange = process.env.RABBITMQ_EXCHANGE;
        const routingKey = process.env.RABBITMQ_ROUTING_KEY;
        const dlqQueue = process.env.RABBITMQ_DLQ_QUEUE || `${queue}_dlq`;
        const dlqExchange = process.env.RABBITMQ_DLQ_EXCHANGE || `${exchange}_dlq`;
        const dlqRoutingKey = process.env.RABBITMQ_DLQ_ROUTING_KEY || `${routingKey}.dlq`;
        // Connect to RabbitMQ
        const connection = await amqplib_1.default.connect(url);
        const channel = await connection.createChannel();
        // Ensure exchange exists
        await channel.assertExchange(exchange, 'topic', { durable: true });
        // Ensure queue exists
        await channel.assertQueue(queue, { durable: true });
        // Bind queue to exchange with routing key
        await channel.bindQueue(queue, exchange, routingKey);
        // Set up DLQ exchange and queue
        await channel.assertExchange(dlqExchange, 'topic', { durable: true });
        await channel.assertQueue(dlqQueue, { durable: true });
        await channel.bindQueue(dlqQueue, dlqExchange, dlqRoutingKey);
        // Set prefetch to process one message at a time
        await channel.prefetch(1);
        logger_1.logger.info(`RabbitMQ connected to ${url}`);
        logger_1.logger.info(`Queue ${queue} bound to exchange ${exchange} with routing key ${routingKey}`);
        logger_1.logger.info(`DLQ ${dlqQueue} bound to exchange ${dlqExchange} with routing key ${dlqRoutingKey}`);
        return { channel, connection, dlqExchange, dlqRoutingKey };
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.logger.error(`RabbitMQ connection error: ${errorMessage}`);
        throw error;
    }
}
/**
 * Send a message to the Dead Letter Queue
 * @param channel - The RabbitMQ channel
 * @param exchange - The DLQ exchange name
 * @param routingKey - The DLQ routing key
 * @param message - The message to send to DLQ
 * @param reason - The reason for sending to DLQ
 * @returns Promise<void>
 */
async function sendToDLQ(channel, exchange, routingKey, message, reason) {
    try {
        // Add metadata about why this message was sent to DLQ
        const dlqMessage = {
            original_message: message,
            error: reason,
            sent_to_dlq_at: new Date().toISOString()
        };
        await channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(dlqMessage)), { persistent: true });
        logger_1.logger.info(`Message sent to DLQ: ${reason}`);
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.logger.error(`Error sending message to DLQ: ${errorMessage}`);
        throw error;
    }
}
//# sourceMappingURL=rabbitmq.js.map