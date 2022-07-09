'use strict';

const {mapUser, mapArticle, getRandomFirstName} = require('./util');
const students = require('./students.json');

// db connection and settings
const connection = require('./config/connection');
let userCollection;
let articleCollection;
let studentCollection;
run();

async function run() {
  await connection.connect();
  await connection.get().dropCollection('users');
  await connection.get().createCollection('users');
  userCollection = connection.get().collection('users');

  await connection.get().dropCollection('articles');
  await connection.get().createCollection('articles');
  articleCollection = connection.get().collection('articles');

  await connection.get().dropCollection('students');
  await connection.get().createCollection('students');
  studentCollection = connection.get().collection('students');

  await example1();
  await example2();
  await example3();
  await example4();

  await example5();
  await example6();
  await example7();
  await example8();
  await example9();

  await example10();

  await example11();
  await example12();
  await example13();
  await example14();
  await example15();
  await example16();
  await example17();
  await connection.close();
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {
  try {
    const users = [];
    const departments = ['a', 'b', 'c'];
    for (let i = 0; i < 2; i++) {
      for (const department of departments) {
        users.push(mapUser({department: department}));
      }
    }
    const result = await userCollection.insertMany(users);
  } catch (err) {
    console.error(err);
  }
}

// - Delete 1 user from department (a)
async function example2() {
  try {
    const result = await userCollection.deleteOne({department: 'a'});
  } catch (err) {
    console.error(err);
  }
}

// - Update firstName for users from department (b)
async function example3() {
  try {
    const result = await userCollection.updateMany(
      {department: 'b'},
      {$set: {firstName: getRandomFirstName()}}
    );
  } catch (err) {
    console.error(err);
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const result = await userCollection.find({department: 'c'}).toArray();
  } catch (err) {
    console.error(err);
  }
}

// #### Articles

// - Create 5 articles per each type (a, b, c)
async function example5() {
  try {
    const articles = [];
    const types = ['a', 'b', 'c'];
    for (let i = 0; i < 5; i++) {
      for (const type of types) {
        articles.push(mapArticle({type: type}));
      }
    }
    const result = await articleCollection.insertMany(articles);
  } catch (err) {
    console.error(err);
  }
}

// - Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function example6() {
  try {
    const result = await articleCollection.updateMany(
      {type: 'a'},
      {$set: {tags: ['tag1-a', 'tag2-a', 'tag3']}}
    );
  } catch (err) {
    console.error(err);
  }
}

// - Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function example7() {
  try {
    const result = await articleCollection.updateMany(
      {type: {$ne: 'a'}},
      {$set: {tags: ['tag2', 'tag3', 'super']}}
    );
  } catch (err) {
    console.error(err);
  }
}

// - Find all articles that contains tags 'tag2' or 'tag1-a'
async function example8() {
  try {
    const result = await articleCollection.find({tags: {$in: ['tag2', 'tag1-a']}}).toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Pull [tag2, tag1-a] from all articles
async function example9() {
  try {
    const result = await articleCollection.updateMany(
      {},
      {$pull: {tags: {$in: ['tag2', 'tag1-a']}}}
    );
  } catch (err) {
    console.error(err);
  }
}

// #### Students

// - Import all data from students.json into student collection
async function example10() {
  try {
    const result = await studentCollection.insertMany(students);
  } catch (err) {
    console.error(err);
  }
}

// #### Students Statistic

// - Find all students who have the worst score for homework, sort by descent
async function example11() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {'scores.type': 'homework'}},
        {$sort: {'scores.score': 1}},
        {$limit: 5},
        {$sort: {'scores.score': -1}}
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have the best score for quiz and the worst for homework, sort by ascending
async function example12() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {
          $group: {
            _id: {_id: '$_id'},
            name: {$first: '$name'},
            quiz: {$first: '$scores.score'},
            homework: {$last: '$scores.score'}
          }
        },
        {$sort: {quiz: -1, homework: 1}},
        {$limit: 5},
        {$sort: {name: 1}}
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have best scope for quiz and exam
async function example13() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {$or: [{'scores.type': 'exam'}, {'scores.type': 'quiz'}]}},
        {$group: {_id: '$_id', name: {$first: '$name'}, quizexamSum: {$sum: '$scores.score'}}},
        {$sort: {quizexamSum: -1}},
        {$limit: 5}
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Calculate the average score for homework for all students
async function example14() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {'scores.type': 'homework'}},
        {$group: {_id: null, average: {$avg: '$scores.score'}}}
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Delete all students that have homework score <= 60
async function example15() {
  try {
    await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {$and: [{'scores.type': 'homework'}, {'scores.score': {$lte: 60}}]}},
        {$set: {markedForDeletion: true}},
        {$merge: {into: 'students', whenMatched: 'replace'}}
      ])
      .toArray();
    await studentCollection.deleteMany({markedForDeletion: true});
  } catch (err) {
    console.error(err);
  }
}

// - Mark students that have quiz score => 80
async function example16() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {$and: [{'scores.type': 'quiz'}, {'scores.score': {$gte: 80}}]}},
        {$set: {markedForQuiz: true}}
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Write a query that group students by 3 categories (calculate the average grade for three subjects)
// - a => (between 0 and 40)
// - b => (between 40 and 60)
// - c => (between 60 and 100)
async function example17() {
  try {
    const result = await studentCollection
      .aggregate([
        {$unwind: '$scores'},
        {$group: {_id: '$_id', average: {$avg: '$scores.score'}}},
        {
          $set: {
            group: {
              $switch: {
                branches: [
                  {
                    case: {$and: [{$gte: ['$average', 0]}, {$lt: ['$average', 40]}]},
                    then: 'a'
                  },
                  {
                    case: {$and: [{$gte: ['$average', 40]}, {$lt: ['$average', 60]}]},
                    then: 'b'
                  },
                  {
                    case: {$and: [{$gte: ['$average', 60]}, {$lt: ['$average', 100]}]},
                    then: 'c'
                  }
                ]
              }
            }
          }
        }
      ])
      .toArray();
  } catch (err) {
    console.error(err);
  }
}
