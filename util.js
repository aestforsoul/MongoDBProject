const faker = require('faker');

const generateUser = ({
  firstName = faker.name.firstName(),
  lastName = faker.name.lastName(),
  department,
  createdAt = new Date()
} = {}) => ({
  firstName,
  lastName,
  department,
  createdAt
});

const generateArticle = ({
  name = faker.lorem.words(3),
  description = faker.lorem.words(10),
  type,
  tags = []
} = {}) => ({
  name,
  description,
  type,
  tags
});

module.exports = {
  mapUser: generateUser,
  mapArticle: generateArticle,
  getRandomFirstName: () => faker.name.firstName()
};
