import Redis from 'ioredis';
import { logger } from '../utils/logger';

export async function connectRedis(): Promise<Redis> {
  try {
    const redisClient = new Redis({
      host: process.env.REDIS_HOST,
      port: parseInt(process.env.REDIS_PORT || '6379', 10),
      password: process.env.REDIS_PASSWORD || undefined,
      db: parseInt(process.env.REDIS_DB || '0', 10),
      retryStrategy: (times: number) => {
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
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Redis connection error: ${errorMessage}`);
    throw error;
  }
}