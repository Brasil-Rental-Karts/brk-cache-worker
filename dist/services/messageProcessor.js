"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.processMessage = processMessage;
const logger_1 = require("../utils/logger");
const rabbitmq_1 = require("./rabbitmq");
/**
 * Process a message from RabbitMQ and update Redis accordingly
 * @param message - The message object from RabbitMQ
 * @param redisClient - The Redis client instance
 * @param rabbitMQChannel - The RabbitMQ channel
 * @param dlqExchange - The DLQ exchange name
 * @param dlqRoutingKey - The DLQ routing key
 * @returns Promise<void>
 */
async function processMessage(message, redisClient, rabbitMQChannel, dlqExchange, dlqRoutingKey) {
    try {
        const { operation, table, timestamp, data } = message;
        if (!operation || !table || !data || !data.id) {
            const reason = 'Invalid message format: missing required fields';
            logger_1.logger.error(reason);
            if (rabbitMQChannel && dlqExchange && dlqRoutingKey) {
                await (0, rabbitmq_1.sendToDLQ)(rabbitMQChannel, dlqExchange, dlqRoutingKey, message, reason);
                return;
            }
            throw new Error(reason);
        }
        // Convert table name to lowercase for consistency in Redis keys
        const tableName = table.toLowerCase();
        // Construct Redis key using the pattern: tableName:id
        const redisKey = `${tableName}:${data.id}`;
        logger_1.logger.info(`Processing ${operation} operation for ${redisKey}`);
        // Check if there's an existing record with timestamp to handle out-of-order messages
        const existingData = await redisClient.get(redisKey);
        if (existingData) {
            const existingRecord = JSON.parse(existingData);
            // If we have a record with a more recent timestamp, skip this update
            if (existingRecord._timestamp && existingRecord._timestamp > timestamp) {
                logger_1.logger.warn(`Skipping outdated message with timestamp ${timestamp} for ${redisKey}`);
                return;
            }
        }
        // Add timestamp to data for future reference
        const dataToStore = { ...data, _timestamp: timestamp };
        switch (operation.toUpperCase()) {
            case 'INSERT':
            case 'UPDATE':
                // For both INSERT and UPDATE, we simply set the data in Redis
                await redisClient.set(redisKey, JSON.stringify(dataToStore));
                logger_1.logger.info(`${operation} operation completed for ${redisKey}`);
                break;
            case 'DELETE':
                // For DELETE, remove the key from Redis
                await redisClient.del(redisKey);
                logger_1.logger.info(`DELETE operation completed for ${redisKey}`);
                break;
            default:
                const reason = `Unsupported operation: ${operation}`;
                logger_1.logger.warn(reason);
                if (rabbitMQChannel && dlqExchange && dlqRoutingKey) {
                    await (0, rabbitmq_1.sendToDLQ)(rabbitMQChannel, dlqExchange, dlqRoutingKey, message, reason);
                    return;
                }
                throw new Error(reason);
        }
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.logger.error(`Error processing message: ${errorMessage}`);
        throw error;
    }
}
//# sourceMappingURL=messageProcessor.js.map