const fs = require('fs');
const path = require('path');
const csvDir = path.join(__dirname, './SDC-data');

const papa = require('papaparse');

const { Client } = require('pg');
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'sdcqa'
});
client
  .connect()
  .then(a => console.log('connected'))
  .catch(err => console.error('connection error', err.stack));

let counter = 0;
let paused = false;


const loadQs = (arr, stream) => {
  const text = `INSERT INTO questions (id, product_id, body, date_written, asker_name, asker_email, reported, helpful) VALUES `;
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    let temp = `(${arr[i].id}, ${arr[i].product_id}, $$${arr[i].body}$$, ${arr[i].date_written}, $$${arr[i].asker_name}$$, $$${arr[i].asker_email}$$, ${!!arr[i].reported}, ${arr[i].helpful})`;
    if (i !== l-1) {temp += `, `; }
    text += temp;
  }

  query(text, stream)
}

const loadAs = (arr, stream) => {
  let text = `INSERT INTO answers (id, question_id, body, date_written, answerer_name, answerer_email, reported, helpful) VALUES `;
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    let temp = `(${arr[i].id}, ${arr[i].question_id}, $$${arr[i].body}$$, ${arr[i].date_written}, $$${arr[i].answerer_name}$$, $$${arr[i].answerer_email}$$, ${!!arr[i].reported}, ${arr[i].helpful})`;
    if (i !== l-1) {temp += `, `; }
    text += temp;
  }

  query(text, stream);
}

const loadPhs = (arr, stream) => {
  let text = `INSERT INTO answers_photos (id, answer_id, url) VALUES `;
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    let temp = `(${arr[i].id}, ${arr[i].answer_id}, $$${arr[i].url}$$)`;
    if (i !== l-1) {temp += `, `; }
    text += temp;
  }

  query(text, stream, arr[0].id);
}

let query = (text, stream, num) => {
  client.query(text)
    .then(res => {
      counter--;
      if (paused && counter < 15) {
        paused = false;
        stream.resume();
        console.log('resuming!!', num);
      }
    })
    .catch(e => {
      counter--;
      console.error(e.stack);
      console.log(num)
   });
}

async function processLineByLine(fl, cb) {
  let file = fs.createReadStream(csvDir + fl);
  papa.parse(file, {
    dynamicTyping: true,
    header: true,
    delimiter: ',',
    chunk: function(chunk) {
      counter++;
      cb(chunk.data, file);
      if (counter > 100) {
          file.pause();
          paused = true;
          console.log('pausing!!')
      }
    },
    complete: function() {
      console.log('done');
    }
  })
};

// processLineByLine('/questions.csv', loadQs);
// processLineByLine('/answers.csv', loadAs);
// processLineByLine('/answers_photos.csv', loadPhs);


/*
CREATE TABLE questions(
id SERIAL PRIMARY KEY,
product_id INT NOT NULL,
body TEXT NOT NULL,
date_written BIGINT NOT NULL,
asker_name TEXT NOT NULL,
asker_email TEXT NOT NULL,
reported BOOL NOT NULL,
helpful INT NOT NULL
);

CREATE TABLE answers(
id SERIAL PRIMARY KEY,
question_id INT REFERENCES questions(id) NOT NULL,
body TEXT NOT NULL,
date_written BIGINT NOT NULL,
answerer_name TEXT NOT NULL,
answerer_email TEXT NOT NULL,
reported BOOL NOT NULL,
helpful INT NOT NULL
);

CREATE TABLE answers_photos(
id SERIAL PRIMARY KEY,
answer_id INT REFERENCES answers(id) NOT NULL,
url TEXT NOT NULL
);
*/