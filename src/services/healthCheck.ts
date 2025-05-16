import express from 'express';
import { logger } from '../utils/logger';

const app = express();
const PORT = process.env.HEALTH_CHECK_PORT || 3000;
const WORKER_URL = process.env.WORKER_URL || 'http://localhost';

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

export function startHealthCheckServer(): void {
  app.listen(PORT, () => {
    logger.info(`Health check server listening on port ${PORT}`);
  });
}

// Function to make self-request to health endpoint
export async function checkHealth(): Promise<void> {
  try {
    const response = await fetch(`${WORKER_URL}:${PORT}/health`);
    if (!response.ok) {
      logger.warn('Health check failed');
    } else {
      logger.debug('Health check successful');
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Health check error: ${errorMessage}`);
  }
}

// Start periodic health checks
export function startPeriodicHealthCheck(): void {
  // Check health every 5 minutes
  setInterval(checkHealth, 1 * 60 * 1000);
}