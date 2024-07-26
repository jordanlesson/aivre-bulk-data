import fs from 'fs'

const items = JSON.parse(fs.readFileSync('temp.json').toString())
const item = items.find(item => !item.PublicationDate)
console.log(item)