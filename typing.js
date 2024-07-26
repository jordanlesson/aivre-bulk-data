import fs from 'fs'
import lineReader from 'line-reader'
import { exit } from 'process'

function isRequired(value) {
  return value.length > 0
}
function isBoolean(value) {
  return value === '' || value === '0' || value === '1'
}
function isNumber(value) {
  return value === '' || !isNaN(value)
}
function isDate(value) {
  return value === '' || /^[\d]{4}-[\d]{2}-[\d]{2}$/i.test(value)
}

let schema = null
let columns = null

let lineIndex = 0
lineReader.eachLine('data.txt', (line, last) => {
  const fields = line.split('||')
  if(schema === null) {
    columns = fields
    schema = fields.reduce((total, current) => {
      total[current] = null
      return total
    }, {})
  } else {
    if(fields.length != columns.length) {
      console.log(fields.length)
      console.log(JSON.stringify(fields))
      throw new Error('column count not match')
    }
    for(let i = 0; i < columns.length; i ++) {
      let current = schema[columns[i]]
      if(current) {
        current.isRequired = current.isRequired && isRequired(fields[i])
        current.isBoolean = current.isBoolean && isBoolean(fields[i])
        current.isNumber = current.isNumber && isNumber(fields[i])
        current.isDate = current.isDate && isDate(fields[i])
      } else {
        current = {
          isRequired: isRequired(fields[i]),
          isBoolean: isBoolean(fields[i]),
          isNumber: isNumber(fields[i]),
          isDate: isDate(fields[i]),
        }
      }
      schema[columns[i]] = current
    }
  }
  lineIndex ++
  if(lineIndex % 10000 === 0) console.log(lineIndex)
  if(last) {
    const mongoSchema = columns.reduce((total, key) => {
      const current = schema[key]
      const field = {}
      if(current.isRequired) field.required = true
      field.type =
        current.isBoolean ? 'Boolean' : 
        current.isNumber ? 'Number' : 
        current.isDate ? 'Date' : 'String'
      total[key] = field
      return total
    }, {})
    fs.writeFileSync('schema.json', JSON.stringify(mongoSchema, null, 2))    
  }
})