const express        = require('express')
const mongodb        = require('mongodb')
const async          = require('async');

const customers      = require('./test_files/m3-customer-data.json');
const customers_addr = require('./test_files/m3-customer-address-data.json');

const app   = express();
const url   = "mongodb://localhost:27017/";

const limit = process.argv[2] || 100;
let tasks = [];

mongodb.MongoClient.connect(url, (err, client) => {
  if(err) throw err;
  console.log("__ Connected to mongodb server");
  const db = client.db("accounts");

  customers.forEach((customer, index, list) => {
    customers[index] = Object.assign(customer, customers_addr[index]);
    if(index % limit === 0) {
      const start = index;
      const end   = (start+limit > customers.length) ? customers.length-1 : start+limit;
      tasks.push((done) => {
        console.log('__ Processing ' + start +"-"+end+" out of "+customers.length);
        db.collection('customers').insert(customers.slice(start, end), (err, result) => {
          done(err, result);
        })
      })
    }
  });

  console.log("__ Launching " + tasks.length + " parallel tasks.");
  const startTime = Date.now();

  async.parallel(tasks, (err, results) => {
    if(err) throw err;
    const endTime = Date.now();
    let diff = endTime - startTime;
    console.log('__ Execution Time: ' + diff +"ms");
    client.close();
  });
});
