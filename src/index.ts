import dotenv from 'dotenv';
dotenv.config();

import { logger } from './utils/logger';
import { connectRabbitMQ } from './services/rabbitmq';
import { connectRedis } from './services/redis';
import { processMessage } from './services/messageProcessor';
import { CacheMessage } from './types';

import { startHealthCheckServer, startPeriodicHealthCheck } from './services/healthCheck';

async function startWorker(): Promise<void> {
  try {
    // Start health check server and periodic checks
    startHealthCheckServer();
    startPeriodicHealthCheck();
    // Connect to Redis
    const redisClient = await connectRedis();
    logger.info('Connected to Redis');

    // Connect to RabbitMQ and start consuming messages
    const { channel, connection, dlqExchange, dlqRoutingKey } = await connectRabbitMQ();
    logger.info('Connected to RabbitMQ');

    const queue = process.env.RABBITMQ_QUEUE as string;
    
    // Set up consumer
    channel.consume(queue, async (msg) => {
      if (msg) {
        try {
          const content = msg.content.toString();
          logger.info(`Received message: ${content}`);
          
          // Process the message
          await processMessage(
            JSON.parse(content) as CacheMessage, 
            redisClient, 
            channel, 
            dlqExchange, 
            dlqRoutingKey
          );
          
          // Acknowledge the message
          channel.ack(msg);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.error(`Error processing message: ${errorMessage}`);
          // Reject the message and don't requeue
          channel.nack(msg, false, false);
        }
      }
    });

    logger.info(`Worker started and consuming from queue: ${queue}`);

    // Handle graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info('Shutting down worker...');
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
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Worker startup failed: ${errorMessage}`);
    process.exit(1);
  }
}

startWorker();