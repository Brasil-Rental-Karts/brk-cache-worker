"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.connectRedis = connectRedis;
const ioredis_1 = __importDefault(require("ioredis"));
const logger_1 = require("../utils/logger");
async function connectRedis() {
    try {
        const redisClient = new ioredis_1.default({
            host: process.env.REDIS_HOST,
            port: parseInt(process.env.REDIS_PORT || '6379', 10),
            password: process.env.REDIS_PASSWORD || undefined,
            db: parseInt(process.env.REDIS_DB || '0', 10),
            retryStrategy: (times) => {
                const delay = Math.min(times * 50, 2000);
                logger_1.logger.info(`Redis connection retry in ${delay}ms`);
                return delay;
            },
        });
        // Test the connection
        await redisClient.ping();
        logger_1.logger.info(`Redis connected to ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`);
        return redisClient;
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.logger.error(`Redis connection error: ${errorMessage}`);
        throw error;
    }
}
//# sourceMappingURL=redis.js.map