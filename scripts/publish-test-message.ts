/**
 * Utility script to publish a test message to RabbitMQ
 * 
 * Usage: 
 * 1. Make sure RabbitMQ is running
 * 2. Run: npx ts-node scripts/publish-test-message.ts
 */

import dotenv from 'dotenv';
dotenv.config();

import amqp from 'amqplib';
import { CacheMessage } from '../src/types';

async function publishTestMessage(): Promise<void> {
  let connection;
  try {
    // Sample message that matches the expected format
    const testMessage: CacheMessage = {
      operation: 'INSERT',
      table: 'Clubs',
      timestamp: Date.now() / 1000, // Current timestamp in seconds
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        name: 'Start Racing Livens',
        foundationDate: null,
        description: 'Clube de kartismo amador',
        logoUrl: null,
        ownerId: 'eaf09940-f0f1-4b23-b597-5d94f3eb74af'
      }
    };

    // Connect to RabbitMQ
    const url = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
    connection = await amqp.connect(url);
    const channel = await connection.createChannel();

    // Get exchange and routing key from env or use defaults
    const exchange = process.env.RABBITMQ_EXCHANGE || 'cache_exchange';
    const routingKey = process.env.RABBITMQ_ROUTING_KEY || 'cache.update';

    // Ensure the exchange exists
    await channel.assertExchange(exchange, 'topic', { durable: true });

    // Publish the message
    const success = channel.publish(
      exchange,
      routingKey,
      Buffer.from(JSON.stringify(testMessage)),
      { persistent: true }
    );

    console.log(`Message published to exchange ${exchange} with routing key ${routingKey}:`);
    console.log(JSON.stringify(testMessage, null, 2));
    console.log(`Publish result: ${success ? 'Success' : 'Failed'}`);

    // Close the connection
    await channel.close();
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`Error publishing test message: ${errorMessage}`);
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

publishTestMessage();