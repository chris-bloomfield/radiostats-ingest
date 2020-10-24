const papa = require('papaparse')
const fs = require('fs')

/**
 * Run Papa Parse on a CSV file in the input directory
 * @param {string} filePath
 */
const parseCsv = (filePath) =>
  new Promise((resolve) => {
    papa.parse(fs.createReadStream(filePath), {
      header: true,
      comments: 'All Individuals 15+ for period ending',
      skipEmptyLines: true,
      dynamicTyping: true,
      complete: ({ data }) => {
        resolve(
          data.map((station) => {
            delete station['']
            return station
          })
        )
      },
    })
  })

module.exports = { parseCsv }
