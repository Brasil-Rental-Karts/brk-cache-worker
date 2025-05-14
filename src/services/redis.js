const Redis = require('ioredis');
const { logger } = require('../utils/logger');

async function connectRedis() {
  try {
    const redisClient = new Redis({
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
      password: process.env.REDIS_PASSWORD || undefined,
      db: process.env.REDIS_DB || 0,
      retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        logger.info(`Redis connection retry in ${delay}ms`);
        return delay;
      },
    });

    // Test the connection
    await redisClient.ping();
    logger.info(`Redis connected to ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`);
    
    return redisClient;
  } catch (error) {
    logger.error(`Redis connection error: ${error.message}`);
    throw error;
  }
}

module.exports = { connectRedis };