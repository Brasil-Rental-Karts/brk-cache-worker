import { Redis } from 'ioredis';
import { Channel } from 'amqplib';
import { logger } from '../utils/logger';
import { sendToDLQ } from './rabbitmq';
import { CacheMessage, RedisData } from '../types';

/**
 * Process a message from RabbitMQ and update Redis accordingly
 * @param message - The message object from RabbitMQ
 * @param redisClient - The Redis client instance
 * @param rabbitMQChannel - The RabbitMQ channel
 * @param dlqExchange - The DLQ exchange name
 * @param dlqRoutingKey - The DLQ routing key
 * @returns Promise<void>
 */
export async function processMessage(
  message: CacheMessage, 
  redisClient: Redis, 
  rabbitMQChannel?: Channel, 
  dlqExchange?: string, 
  dlqRoutingKey?: string
): Promise<void> {
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
      const existingRecord = JSON.parse(existingData) as RedisData;
      // If we have a record with a more recent timestamp, skip this update
      if (existingRecord._timestamp && existingRecord._timestamp > timestamp) {
        logger.warn(`Skipping outdated message with timestamp ${timestamp} for ${redisKey}`);
        return;
      }
    }
    
    // Add timestamp to data for future reference
    const dataToStore: RedisData = { ...data, _timestamp: timestamp };
    
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
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error processing message: ${errorMessage}`);
    throw error;
  }
}