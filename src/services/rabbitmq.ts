import amqp, { Channel, Connection as AmqpConnection } from 'amqplib';
import { ChannelModel } from 'amqplib';
import { logger } from '../utils/logger';
import { RabbitMQConnection, DLQMessage, CacheMessage } from '../types';

export async function connectRabbitMQ(): Promise<RabbitMQConnection> {
  try {
    const url = process.env.RABBITMQ_URL as string;
    const queue = process.env.RABBITMQ_QUEUE as string;
    const exchange = process.env.RABBITMQ_EXCHANGE as string;
    const routingKey = process.env.RABBITMQ_ROUTING_KEY as string;
    const dlqQueue = process.env.RABBITMQ_DLQ_QUEUE || `${queue}_dlq`;
    const dlqExchange = process.env.RABBITMQ_DLQ_EXCHANGE || `${exchange}_dlq`;
    const dlqRoutingKey = process.env.RABBITMQ_DLQ_ROUTING_KEY || `${routingKey}.dlq`;

    // Connect to RabbitMQ
    const connection = await amqp.connect(url) as unknown as ChannelModel;
    const channel = await connection.createChannel();

    // Ensure exchange exists
    await channel.assertExchange(exchange, 'topic', { durable: true });

    // Ensure queue exists
    await channel.assertQueue(queue, { durable: true });

    // Bind queue to exchange with routing key
    await channel.bindQueue(queue, exchange, routingKey);

    // Set up DLQ exchange and queue
    await channel.assertExchange(dlqExchange, 'topic', { durable: true });
    await channel.assertQueue(dlqQueue, { durable: true });
    await channel.bindQueue(dlqQueue, dlqExchange, dlqRoutingKey);

    // Set prefetch to process one message at a time
    await channel.prefetch(1);

    logger.info(`RabbitMQ connected to ${url}`);
    logger.info(`Queue ${queue} bound to exchange ${exchange} with routing key ${routingKey}`);
    logger.info(`DLQ ${dlqQueue} bound to exchange ${dlqExchange} with routing key ${dlqRoutingKey}`);

    return { channel, connection, dlqExchange, dlqRoutingKey };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`RabbitMQ connection error: ${errorMessage}`);
    throw error;
  }
}

/**
 * Send a message to the Dead Letter Queue
 * @param channel - The RabbitMQ channel
 * @param exchange - The DLQ exchange name
 * @param routingKey - The DLQ routing key
 * @param message - The message to send to DLQ
 * @param reason - The reason for sending to DLQ
 * @returns Promise<void>
 */
export async function sendToDLQ(
  channel: Channel, 
  exchange: string, 
  routingKey: string, 
  message: CacheMessage, 
  reason: string
): Promise<void> {
  try {
    // Add metadata about why this message was sent to DLQ
    const dlqMessage: DLQMessage = {
      original_message: message,
      error: reason,
      sent_to_dlq_at: new Date().toISOString()
    };

    await channel.publish(
      exchange,
      routingKey,
      Buffer.from(JSON.stringify(dlqMessage)),
      { persistent: true }
    );

    logger.info(`Message sent to DLQ: ${reason}`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error sending message to DLQ: ${errorMessage}`);
    throw error;
  }
}