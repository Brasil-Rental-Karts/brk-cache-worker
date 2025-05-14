const { processMessage } = require('../src/services/messageProcessor');

// Mock dependencies
const mockRedisClient = {
  get: jest.fn(),
  set: jest.fn(),
  del: jest.fn()
};

const mockLogger = {
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn()
};

// Mock the logger module
jest.mock('../src/utils/logger', () => ({
  logger: mockLogger
}));

describe('Message Processor', () => {
  beforeEach(() => {
    // Clear all mocks before each test
    jest.clearAllMocks();
  });

  test('should process INSERT operation correctly', async () => {
    // Arrange
    const message = {
      operation: 'INSERT',
      table: 'Clubs',
      timestamp: 1747002033.158324,
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce',
        name: 'Start Racing Livens',
        description: 'Clube de kartismo amador'
      }
    };

    // Act
    await processMessage(message, mockRedisClient);

    // Assert
    expect(mockRedisClient.set).toHaveBeenCalledWith(
      'clubs:dae88056-e830-461a-b299-000aac64b4ce',
      expect.any(String)
    );
    const storedData = JSON.parse(mockRedisClient.set.mock.calls[0][1]);
    expect(storedData).toEqual({
      ...message.data,
      _timestamp: message.timestamp
    });
  });

  test('should process UPDATE operation correctly', async () => {
    // Arrange
    const message = {
      operation: 'UPDATE',
      table: 'Clubs',
      timestamp: 1747002033.158324,
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce',
        name: 'Updated Racing Club',
        description: 'Updated description'
      }
    };

    // Act
    await processMessage(message, mockRedisClient);

    // Assert
    expect(mockRedisClient.set).toHaveBeenCalledWith(
      'clubs:dae88056-e830-461a-b299-000aac64b4ce',
      expect.any(String)
    );
  });

  test('should process DELETE operation correctly', async () => {
    // Arrange
    const message = {
      operation: 'DELETE',
      table: 'Clubs',
      timestamp: 1747002033.158324,
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce'
      }
    };

    // Act
    await processMessage(message, mockRedisClient);

    // Assert
    expect(mockRedisClient.del).toHaveBeenCalledWith(
      'clubs:dae88056-e830-461a-b299-000aac64b4ce'
    );
  });

  test('should skip processing if message has older timestamp', async () => {
    // Arrange
    const message = {
      operation: 'UPDATE',
      table: 'Clubs',
      timestamp: 1000, // Older timestamp
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce',
        name: 'Old Data'
      }
    };

    // Mock existing data with newer timestamp
    mockRedisClient.get.mockResolvedValue(JSON.stringify({
      id: 'dae88056-e830-461a-b299-000aac64b4ce',
      name: 'Current Data',
      _timestamp: 2000 // Newer timestamp
    }));

    // Act
    await processMessage(message, mockRedisClient);

    // Assert
    expect(mockRedisClient.set).not.toHaveBeenCalled();
    expect(mockLogger.warn).toHaveBeenCalled();
  });

  test('should throw error for invalid message format', async () => {
    // Arrange
    const invalidMessage = {
      operation: 'INSERT',
      // Missing table and data
    };

    // Act & Assert
    await expect(processMessage(invalidMessage, mockRedisClient))
      .rejects
      .toThrow('Invalid message format');
  });

  test('should throw error for unsupported operation', async () => {
    // Arrange
    const message = {
      operation: 'INVALID_OP',
      table: 'Clubs',
      timestamp: 1747002033.158324,
      data: {
        id: 'dae88056-e830-461a-b299-000aac64b4ce'
      }
    };

    // Act & Assert
    await expect(processMessage(message, mockRedisClient))
      .rejects
      .toThrow('Unsupported operation');
  });
});