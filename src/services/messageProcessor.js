const { logger } = require('../utils/logger');
const { sendToDLQ } = require('./rabbitmq');

/**
 * Process a message from RabbitMQ and update Redis accordingly
 * @param {Object} message - The message object from RabbitMQ
 * @param {Object} redisClient - The Redis client instance
 * @param {Object} rabbitMQChannel - The RabbitMQ channel
 * @param {string} dlqExchange - The DLQ exchange name
 * @param {string} dlqRoutingKey - The DLQ routing key
 * @returns {Promise<void>}
 */
async function processMessage(message, redisClient, rabbitMQChannel, dlqExchange, dlqRoutingKey) {
  try {
    const { operation, table, timestamp, data } = message;
    
    if (!operation || !table || !data || !data.id) {
      const reason = 'Invalid message format: missing required fields';
      logger.error(reason);
      if (rabbitMQChannel && dlqExchange && dlqRoutingKey) {
        await sendToDLQ(rabbitMQChannel, dlqExchange, dlqRoutingKey, message, reason);
        return;
      }
      throw new Error(reason);
    }
    
    // Convert table name to lowercase for consistency in Redis keys
    const tableName = table.toLowerCase();
    // Construct Redis key using the pattern: tableName:id
    const redisKey = `${tableName}:${data.id}`;
    
    logger.info(`Processing ${operation} operation for ${redisKey}`);
    
    // Check if there's an existing record with timestamp to handle out-of-order messages
    const existingData = await redisClient.get(redisKey);
    if (existingData) {
      const existingRecord = JSON.parse(existingData);
      // If we have a record with a more recent timestamp, skip this update
      if (existingRecord._timestamp && existingRecord._timestamp > timestamp) {
        logger.warn(`Skipping outdated message with timestamp ${timestamp} for ${redisKey}`);
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
        logger.info(`${operation} operation completed for ${redisKey}`);
        break;
        
      case 'DELETE':
        // For DELETE, remove the key from Redis
        await redisClient.del(redisKey);
        logger.info(`DELETE operation completed for ${redisKey}`);
        break;
        
      default:
        const reason = `Unsupported operation: ${operation}`;
        logger.warn(reason);
        if (rabbitMQChannel && dlqExchange && dlqRoutingKey) {
          await sendToDLQ(rabbitMQChannel, dlqExchange, dlqRoutingKey, message, reason);
          return;
        }
        throw new Error(reason);
    }
  } catch (error) {
    logger.error(`Error processing message: ${error.message}`);
    throw error;
  }
}

module.exports = { processMessage };