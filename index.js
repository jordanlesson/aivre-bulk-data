import fs from 'fs'
import mongoose from 'mongoose';
import LineByLineReader from 'line-by-line';

const FIELD_TYPES = {
  'String': String,
  'Number': Number,
  "Date": Date,
  "Boolean": Boolean,
}
const START_OFFSET = 195900

const schema = JSON.parse(fs.readFileSync('schema.json').toString())
for(let key in schema) {
  const field = schema[key]
  field.type = FIELD_TYPES[field.type]
}

const mongoSchema = new mongoose.Schema(schema);
let mongoModel = null

const connect = async () => {
  const connection = await mongoose.connect(`mongodb+srv://jordanlesson:KA-UFazWNLFP%406d@aivre.yg4yye0.mongodb.net/AIVRE`);
  console.log('MongoDB successfully connected!');
  return connection
};
const disconnect = async () => {
  await mongoose.disconnect();
};

const batchCount = 100
let currentBatch = []

let lineIndex = 0

async function postBatch() {
  if(currentBatch.length === 0) return
  await mongoModel.insertMany(currentBatch)
  console.log('posted', lineIndex)
}

(async () => {
  const connection = await connect()
  mongoModel = connection.model('AttomBulk', mongoSchema);
  let columns = null

  const lr = new LineByLineReader('data.txt');

  lr.on('error', function (err) {
    console.log(err)
  });

  lr.on('line', function (line) {
    const fields = line.split('||')
    if(columns === null) {
      columns = fields
    } else {
      if(fields.length !== columns.length)
        return
      lineIndex ++
      if(lineIndex < START_OFFSET)
        return
      const document = columns.reduce((total, key, index) => {
        let value = fields[index]
        if(value) {
          if(schema[key].type === Boolean)
            value = Boolean(value)
          if(schema[key].type === Number)
            value = Number(value)
          else if(schema[key].type === Date)
            value = new Date(value)
          total[key] = value
        }
        return total
      }, {})
      currentBatch.push(document)
      if(currentBatch.length === batchCount) {
        lr.pause();
        postBatch().then(() => {
          currentBatch = []
          lr.resume()
        })
      }
    }
  });

  lr.on('end', function () {
    postBatch().then(() => {
      disconnect().then(() => {
        console.log('Mongodb successfuly disconnected')
      })
    })
  });
})()