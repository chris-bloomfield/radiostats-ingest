require('dotenv').config()

const ingest = require('./src/ingestStationDataToDb')

if (!process.env.DB_URL) {
  throw new Error('Missing DB_URL from .env files')
}

ingest()
