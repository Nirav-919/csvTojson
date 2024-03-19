const express = require('express');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('pg');
require('dotenv').config();

const app = express();

const csvFilePath = process.env.CSV_FILE_PATH;
const PG_HOST = process.env.PG_HOST;
const PG_PORT = process.env.PG_PORT;
const PG_USER = process.env.PG_USER;
const PG_PASSWORD = process.env.PG_PASSWORD;
const PG_DATABASE = process.env.PG_DATABASE;

const client = new Client({
  host: PG_HOST,
  port: PG_PORT,
  user: PG_USER,
  password: PG_PASSWORD,
  database: PG_DATABASE,
});

client.connect((err) => {
  if (err) {
    console.error('Error connecting to database:', err);
    process.exit(1); // Exit on connection error
  } else {
    console.log('Connected to database successfully');
  }
});

function parseCsvRow(row, headers) {
  const parsedRow = {};
  let currentObject = parsedRow;
  for (let i = 0; i < headers.length; i++) {
    const path = headers[i].split('.');
    for (let j = 0; j < path.length - 1; j++) {
      currentObject = currentObject[path[j]] = currentObject[path[j]] || {};
    }
    currentObject[path[path.length - 1]] = row[i];
  }
  return parsedRow;
}

async function convertToAndStoreData(req, res) {
  try {
    const jsonData = [];
    const readStream = fs.createReadStream(csvFilePath)
      .pipe(csv());

    let headers = []; // To store headers from the first line
    let isHeader = true; // Flag to identify header row

    for await (const row of readStream) {
      if (isHeader) {
        headers = row;
        isHeader = false;
        continue;
      }

      const parsedData = parseCsvRow(row, headers);
      jsonData.push(parsedData);

      // Prepare data for insertion (separate mandatory and additional)
      const name = parsedData.name?.firstName + ' ' + parsedData.name?.lastName;
      const age = parseInt(row.age);
      const address = JSON.stringify(parsedData.address);
      delete parsedData.name;
      delete parsedData.age;
      delete parsedData.address;
      const additionalInfo = JSON.stringify(parsedData);

      const query = `
        INSERT INTO users (name, age, address, additional_info)
        VALUES ($1, $2, $3, $4)
      `;

      await client.query(query, [name, age, address, additionalInfo]);
    }

    calculateAgeDistribution();
    res.json({ message: 'CSV converted and data uploaded to DB' });
  } catch (error) {
    console.error(error);
    res.status(500).send('Error converting CSV or uploading data');
  }
}

function calculateAgeDistribution() {
  const query = `
    SELECT floor(age / 20) * 20 AS age_group, COUNT(*) AS total
    FROM users
    GROUP BY floor(age / 20) * 20
    ORDER BY age_group;
  `;

  client.query(query, (err, res) => {
    if (err) {
      console.error(err);
      return;
    }

    console.log('\nAge Distribution Report:');
    console.log('Age-Group       % Distribution');
    for (const row of res.rows) {
      const percentage = (row.total / res.rowCount * 100).toFixed(2);
      console.log(`${row.age_group} \t\t  ${percentage}`);
    }
  });
}

app.get('/convert-csv', convertToAndStoreData);

app.listen(3000, () => console.log('Server listening on port 3000'));
