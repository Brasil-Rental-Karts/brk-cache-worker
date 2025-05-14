require('dotenv').config();
const { logger } = require('./utils/logger');
const { connectRabbitMQ } = require('./services/rabbitmq');
const { connectRedis } = require('./services/redis');
const { processMessage } = require('./services/messageProcessor');

async function startWorker() {
  try {
    // Connect to Redis
    const redisClient = await connectRedis();
    logger.info('Connected to Redis');

    // Connect to RabbitMQ and start consuming messages
    const { channel, connection, dlqExchange, dlqRoutingKey } = await connectRabbitMQ();
    logger.info('Connected to RabbitMQ');

    const queue = process.env.RABBITMQ_QUEUE;
    
    // Set up consumer
    channel.consume(queue, async (msg) => {
      if (msg) {
        try {
          const content = msg.content.toString();
          logger.info(`Received message: ${content}`);
          
          // Process the message
          await processMessage(JSON.parse(content), redisClient, channel, dlqExchange, dlqRoutingKey);
          
          // Acknowledge the message
          channel.ack(msg);
        } catch (error) {
          logger.error(`Error processing message: ${error.message}`);
          // Reject the message and don't requeue
          channel.nack(msg, false, false);
        }
      }
    });

    logger.info(`Worker started and consuming from queue: ${queue}`);

    // Handle graceful shutdown
    const shutdown = async () => {
      logger.info('Shutting down worker...');
      await channel.close();
      await connection.close();
      await redisClient.quit();
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  } catch (error) {
    logger.error(`Worker startup failed: ${error.message}`);
    process.exit(1);
  }
}

startWorker();