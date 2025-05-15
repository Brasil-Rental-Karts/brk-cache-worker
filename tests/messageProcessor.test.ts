import { Redis } from 'ioredis';
import { Channel } from 'amqplib';
import { processMessage } from '../src/services/messageProcessor';
import { CacheMessage } from '../src/types';

// Mock dependencies
jest.mock('../src/utils/logger', () => ({
  logger: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

jest.mock('../src/services/rabbitmq', () => ({
  sendToDLQ: jest.fn(),
}));

describe('Message Processor', () => {
  let mockRedisClient: Partial<Redis>;
  let mockRabbitMQChannel: Partial<Channel>;
  
  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    
    // Setup Redis mock
    mockRedisClient = {
      get: jest.fn(),
      set: jest.fn(),
      del: jest.fn(),
    };
    
    // Setup RabbitMQ channel mock
    mockRabbitMQChannel = {
      publish: jest.fn(),
    };
  });
  
  it('should process INSERT operation correctly', async () => {
    // Arrange
    const message: CacheMessage = {
      operation: 'INSERT',
      table: 'clubs',
      timestamp: Date.now() / 1000,
      data: {
        id: '123',
        name: 'Test Club',
      },
    };
    
    // Act
    await processMessage(
      message,
      mockRedisClient as Redis,
      mockRabbitMQChannel as Channel,
      'dlq_exchange',
      'dlq_routing_key'
    );
    
    // Assert
    expect(mockRedisClient.set).toHaveBeenCalledWith(
      'clubs:123',
      expect.any(String)
    );
  });
  
  it('should process DELETE operation correctly', async () => {
    // Arrange
    const message: CacheMessage = {
      operation: 'DELETE',
      table: 'clubs',
      timestamp: Date.now() / 1000,
      data: {
        id: '123',
      },
    };
    
    // Act
    await processMessage(
      message,
      mockRedisClient as Redis,
      mockRabbitMQChannel as Channel,
      'dlq_exchange',
      'dlq_routing_key'
    );
    
    // Assert
    expect(mockRedisClient.del).toHaveBeenCalledWith('clubs:123');
  });
  
  it('should skip processing if existing record has newer timestamp', async () => {
    // Arrange
    const message: CacheMessage = {
      operation: 'UPDATE',
      table: 'clubs',
      timestamp: 1000, // Older timestamp
      data: {
        id: '123',
        name: 'Old Data',
      },
    };
    
    // Mock existing data with newer timestamp
    (mockRedisClient.get as jest.Mock).mockResolvedValue(
      JSON.stringify({
        id: '123',
        name: 'Newer Data',
        _timestamp: 2000, // Newer timestamp
      })
    );
    
    // Act
    await processMessage(
      message,
      mockRedisClient as Redis,
      mockRabbitMQChannel as Channel,
      'dlq_exchange',
      'dlq_routing_key'
    );
    
    // Assert
    expect(mockRedisClient.set).not.toHaveBeenCalled();
  });

  it('should send to DLQ for invalid message format', async () => {
    // Arrange
    const invalidMessage = {
      operation: 'INSERT',
      // Missing table and data fields
    } as CacheMessage;
    
    // Act
    await processMessage(
      invalidMessage,
      mockRedisClient as Redis,
      mockRabbitMQChannel as Channel,
      'dlq_exchange',
      'dlq_routing_key'
    );
    
    // Assert
    const { sendToDLQ } = require('../src/services/rabbitmq');
    expect(sendToDLQ).toHaveBeenCalledWith(
      mockRabbitMQChannel,
      'dlq_exchange',
      'dlq_routing_key',
      invalidMessage,
      expect.any(String)
    );
  });

  it('should send to DLQ for unsupported operation', async () => {
    // Arrange
    const message = {
      operation: 'INVALID_OP' as any,
      table: 'clubs',
      timestamp: Date.now() / 1000,
      data: {
        id: '123',
      },
    };
    
    // Act
    await processMessage(
      message,
      mockRedisClient as Redis,
      mockRabbitMQChannel as Channel,
      'dlq_exchange',
      'dlq_routing_key'
    );
    
    // Assert
    const { sendToDLQ } = require('../src/services/rabbitmq');
    expect(sendToDLQ).toHaveBeenCalledWith(
      mockRabbitMQChannel,
      'dlq_exchange',
      'dlq_routing_key',
      message,
      expect.any(String)
    );
  });
});