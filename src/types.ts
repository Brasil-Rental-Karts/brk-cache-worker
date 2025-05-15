/**
 * Type definitions for the BRK Cache Worker
 */

// Redis client type from ioredis
import { Redis } from 'ioredis';
import { Channel } from 'amqplib';

// Message structure received from RabbitMQ
export interface CacheMessage {
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;
  timestamp: number;
  data: {
    id: string;
    [key: string]: any;
  };
}

// RabbitMQ connection result
export interface RabbitMQConnection {
  channel: Channel;
  // Using ChannelModel which has the close() method properly defined
  connection: import('amqplib').ChannelModel;
  dlqExchange: string;
  dlqRoutingKey: string;
}

// Dead Letter Queue message with metadata
export interface DLQMessage {
  original_message: CacheMessage;
  error: string;
  sent_to_dlq_at: string;
}

// Redis data with timestamp
export interface RedisData {
  _timestamp: number;
  [key: string]: any;
}