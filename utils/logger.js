const pino = require('pino');

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  
  // Enable script name and line number tracking
  caller: true,
  
  // Format the timestamp to ISO
  timestamp: pino.stdTimeFunctions.isoTime,
  
  // Terminal output styling (Optional - for local development)
  transport: process.env.NODE_ENV !== 'production' 
    ? {
        target: 'pino-pretty',
        options: {
          colorize: true,
          ignore: 'pid,hostname', // Hide process ID and hostname to keep output clean
        }
      }
    : undefined
});

module.exports = logger;