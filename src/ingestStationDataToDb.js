/**
 * Purges any existing data and creates three collections on the specified DB
 * - results - each document = one set of quarterly results for a station
 * - nameChanges - each document = one station name changing to another
 * - stations - each document = one station
 *
 * Each station document contains references to all its results and nameChanges
 */

const { MongoClient } = require('mongodb')
const { map, pipe } = require('ramda')
const dayjs = require('dayjs')
const customParseFormat = require('dayjs/plugin/customParseFormat')
dayjs.extend(customParseFormat)

const { getResultsFromFiles, getNameChangesFromFiles } = require('../lib/getDataFromFiles')

const resultsPath = `${__dirname}/../input`
const nameChangePath = `${__dirname}/../data/nameChanges`

const getPreviousQuarterDate = (date) => dayjs(date).subtract(3, 'month').endOf('month')

const ingest = async () => {
  try {
    const uri = process.env.DB_URL
    const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true })

    const results = await getResultsFromFiles(resultsPath)

    console.log('Built results array')

    let nameChangeErrors = []
    const changes = pipe(
      getNameChangesFromFiles,
      map((nameChange) => {
        // Check name change 'to' field matches result data
        if (
          !results.find(
            (res) =>
              res.surveyEndDate.getTime() === nameChange.surveyEndDate.getTime() &&
              res.stationGroup === nameChange.to
          )
        ) {
          nameChangeErrors.push(
            `${nameChange.surveyEndDate} - new name ${nameChange.to} not found in results`
          )
        }
        // Check name change 'from' field matches result data
        const previousQuarterDate = getPreviousQuarterDate(nameChange.surveyEndDate)
        if (
          !results.find(
            (res) =>
              res.surveyEndDate.getTime() === previousQuarterDate.valueOf() &&
              res.stationGroup === nameChange.from
          )
        ) {
          nameChangeErrors.push(
            `${nameChange.surveyEndDate} - old name ${nameChange.from} not found previous Q results`
          )
        }
        return nameChange
      })
    )(nameChangePath)

    if (nameChangeErrors.length > 0) {
      console.log('Name change errors - skipping DB updates')
      console.log(nameChangeErrors)
      return
    } else {
      console.log('Verified name changes')
    }

    // Connect to DB & create results / nameChanges collections
    await client.connect()
    const resultsCollection = client.db('radio-stats').collection('results')
    const changesCollection = client.db('radio-stats').collection('nameChanges')
    const stationsCollection = client.db('radio-stats').collection('stations')
    await Promise.all([
      resultsCollection.deleteMany({}),
      changesCollection.deleteMany({}),
      stationsCollection.deleteMany({}),
    ])
    console.log('Deleted existing data')
    const insertedResults = await resultsCollection.insertMany(results || [])
    const insertedChanges = await changesCollection.insertMany(changes || [])

    // Generate station documents array & add result / nameChange IDs
    const stations = insertedResults.ops.reduce((acc, result) => {
      // If station is already in acc we can just add this result to it
      const stationIndex = acc.findIndex(
        (station) =>
          station.currentName === result.stationGroup &&
          // Check that results data exists up to previous quarter
          // (avoid inadvertently stitching together two sets of results with the same name)
          station.results[station.results.length - 1].surveyEndDate.getTime() ===
            getPreviousQuarterDate(result.surveyEndDate).valueOf()
      )
      if (stationIndex >= 0) {
        acc[stationIndex].results.push(result)
        acc[stationIndex].latestResult = new Date(result.surveyEndDate)
        return acc
      }

      // Check if name has changed - if it has we can add the result + nameChange
      // to the existing station
      const nameChange = insertedChanges.ops.find(
        (change) =>
          change.to === result.stationGroup &&
          change.surveyEndDate.getTime() === result.surveyEndDate.getTime()
      )
      if (nameChange) {
        const indexOfstationToRename = acc.findIndex(
          (station) =>
            station.currentName === nameChange.from &&
            station.results[station.results.length - 1].surveyEndDate.getTime() ===
              getPreviousQuarterDate(nameChange.surveyEndDate).valueOf()
        )
        if (indexOfstationToRename >= 0) {
          acc[indexOfstationToRename].nameChanges.push(nameChange._id)
          acc[indexOfstationToRename].results.push(result)
          acc[indexOfstationToRename].currentName = result.stationGroup
          acc[indexOfstationToRename].latestResult = new Date(result.surveyEndDate)
          return acc
        }
      }

      // Otherwise it's a new station
      acc.push({
        currentName: result.stationGroup,
        results: [result],
        nameChanges: [],
        firstResult: new Date(result.surveyEndDate),
        latestResult: new Date(result.surveyEndDate),
      })

      return acc
    }, [])

    // Strip results data out of station results - we just need IDs
    const stationsToInsert = stations.map((station) => ({
      ...station,
      results: station.results.map((result) => result._id),
    }))
    await stationsCollection.insertMany(stationsToInsert)

    await client.close()
    console.log(
      `Inserted ${stations.length} stations, ${results.length} results and ${changes.length} name changes`
    )
  } catch (err) {
    console.error(err)
  } finally {
    process.exit()
  }
}

module.exports = ingest
