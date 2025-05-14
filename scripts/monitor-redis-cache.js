/**
 * Utility script to monitor Redis cache content
 * 
 * Usage: 
 * 1. Make sure Redis is running
 * 2. Run: node scripts/monitor-redis-cache.js [pattern]
 *    - pattern: Optional Redis key pattern to filter (default: *)
 */

require('dotenv').config();
const Redis = require('ioredis');

async function monitorRedisCache() {
  let redisClient;
  try {
    // Connect to Redis
    redisClient = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: process.env.REDIS_PORT || 6379,
      password: process.env.REDIS_PASSWORD || undefined,
      db: process.env.REDIS_DB || 0,
    });

    // Get pattern from command line args or use default
    const pattern = process.argv[2] || '*';
    console.log(`Scanning Redis for keys matching pattern: ${pattern}`);

    // Scan for keys matching the pattern
    let cursor = '0';
    let keys = [];
    
    do {
      const result = await redisClient.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      cursor = result[0];
      keys = keys.concat(result[1]);
    } while (cursor !== '0');

    console.log(`Found ${keys.length} keys matching pattern: ${pattern}`);

    // Get values for each key
    if (keys.length > 0) {
      for (const key of keys) {
        const type = await redisClient.type(key);
        let value;

        switch (type) {
          case 'string':
            value = await redisClient.get(key);
            try {
              // Try to parse as JSON
              const jsonValue = JSON.parse(value);
              value = jsonValue;
            } catch (e) {
              // Not JSON, keep as string
            }
            break;
          case 'hash':
            value = await redisClient.hgetall(key);
            break;
          case 'list':
            value = await redisClient.lrange(key, 0, -1);
            break;
          case 'set':
            value = await redisClient.smembers(key);
            break;
          case 'zset':
            value = await redisClient.zrange(key, 0, -1, 'WITHSCORES');
            break;
          default:
            value = `[${type} type]`;
        }

        console.log('\n-----------------------------------');
        console.log(`Key: ${key}`);
        console.log(`Type: ${type}`);
        console.log('Value:');
        console.log(typeof value === 'object' ? JSON.stringify(value, null, 2) : value);
      }
    }

  } catch (error) {
    console.error(`Error monitoring Redis cache: ${error.message}`);
  } finally {
    if (redisClient) {
      await redisClient.quit();
    }
  }
}

monitorRedisCache();