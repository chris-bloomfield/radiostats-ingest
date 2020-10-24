const fs = require('fs')
const { filter, map, pipe, unnest } = require('ramda')
const dayjs = require('dayjs')
const { parseCsv } = require('./parseCsv')

const getFullDate = (yyyymm) => dayjs(yyyymm, 'YYYYMM').endOf('month')

const getResultsFromFiles = async (resultsPath) => {
  const resultsFilenames = fs.readdirSync(resultsPath)

  // Parse CSV files
  const csvData = await Promise.all(
    pipe(
      filter(
        (fileName) =>
          fileName.startsWith('rajar_quarterly_listening_report_to_') && fileName.endsWith('.csv')
      ),
      map(async (fileName) => {
        const surveyEndDate = getFullDate(fileName.slice(36, 42))
        const parsed = await parseCsv(`${resultsPath}/${fileName}`)
        // Add survey end date to parsed results
        return map((result) => ({ ...result, surveyEndDate: new Date(surveyEndDate) }))(parsed)
      })
    )(resultsFilenames)
  )

  // Take parsed CSV data and transform into an array of results
  return pipe(
    map((resultSet) =>
      map((result) => ({
        surveyEndDate: result.surveyEndDate,
        stationGroup: result['Station/Group'],
        surveyPeriod: result['Survey Period'],
        population: result['Population 000s'] * 1000,
        reach: result['Reach 000s'] * 1000,
        reachPercent: result['Reach Percent'],
        avgHoursPerHead: result['Average Hours Per Head'],
        avgHoursPerListener: result['Average Hours Per Listener'],
        totalHours: result['Total Hours 000s'] * 1000,
        TSAListeningSharePercent: result['Listening Share In TSA %'],
      }))(resultSet)
    ),
    unnest
  )(csvData)
}

const getNameChangesFromFiles = (nameChangePath) => {
  const nameChangeFilenames = fs.readdirSync(nameChangePath)
  const changes = pipe(
    filter((fileName) => fileName.match(/[0-9]{6}(\.json)/)),
    map((fileName) => {
      const surveyEndDate = getFullDate(fileName.slice(0, 6))
      const fileData = JSON.parse(fs.readFileSync(`${nameChangePath}/${fileName}`))
      return map((result) => ({
        surveyEndDate: new Date(surveyEndDate),
        from: result[0],
        to: result[1],
      }))(fileData)
    }),
    unnest
  )(nameChangeFilenames)
  return changes
}

module.exports = { getResultsFromFiles, getNameChangesFromFiles }
