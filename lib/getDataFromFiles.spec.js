const { getResultsFromFiles, getNameChangesFromFiles } = require('./getDataFromFiles')
const assert = require('assert')

describe('lib/getResultsFromFiles()', () => {
  it('converts CSV data into an array of results', async () => {
    const results = await getResultsFromFiles(`${__dirname}/../fixtures/results`)
    assert.deepStrictEqual(results, [
      {
        surveyEndDate: new Date('2008-12-31T23:59:59.999Z'),
        stationGroup: 'Test Station',
        surveyPeriod: 'Q',
        population: 1401000,
        reach: 241000,
        reachPercent: 21,
        avgHoursPerHead: 2.1,
        avgHoursPerListener: 12.51,
        totalHours: 2801000,
        TSAListeningSharePercent: 8.51,
      },
      {
        surveyEndDate: new Date('2008-12-31T23:59:59.999Z'),
        stationGroup: 'Test Station 2',
        surveyPeriod: 'Q',
        population: 1402000,
        reach: 242000,
        reachPercent: 22,
        avgHoursPerHead: 2.2,
        avgHoursPerListener: 12.52,
        totalHours: 2802000,
        TSAListeningSharePercent: 8.52,
      },
      {
        surveyEndDate: new Date('2009-03-31T22:59:59.999Z'),
        stationGroup: 'Test Station',
        surveyPeriod: 'Q',
        population: 1401000,
        reach: 241000,
        reachPercent: 21,
        avgHoursPerHead: 2.1,
        avgHoursPerListener: 12.51,
        totalHours: 2801000,
        TSAListeningSharePercent: 8.51,
      },
      {
        surveyEndDate: new Date('2009-03-31T22:59:59.999Z'),
        stationGroup: 'Test Station 2',
        surveyPeriod: 'Q',
        population: 1402000,
        reach: 242000,
        reachPercent: 22,
        avgHoursPerHead: 2.2,
        avgHoursPerListener: 12.52,
        totalHours: 2802000,
        TSAListeningSharePercent: 8.52,
      },
    ])
  })
})

describe('lib/getNameChangesFromFiles()', () => {
  it('converts name change JSON data & transforms to the correct format', async () => {
    const nameChanges = await getNameChangesFromFiles(`${__dirname}/../fixtures/nameChanges`)
    assert.deepStrictEqual(nameChanges, [
      {
        surveyEndDate: new Date('2008-12-31T23:59:59.999Z'),
        from: 'Station 1 old name',
        to: 'Station 1 new name',
      },
      {
        surveyEndDate: new Date('2008-12-31T23:59:59.999Z'),
        from: 'Station 2 old name',
        to: 'Station 2 new name',
      },
      {
        surveyEndDate: new Date('2009-03-31T22:59:59.999Z'),
        from: 'Station 1 old name',
        to: 'Station 1 new name',
      },
      {
        surveyEndDate: new Date('2009-03-31T22:59:59.999Z'),
        from: 'Station 2 old name',
        to: 'Station 2 new name',
      },
    ])
  })
})
