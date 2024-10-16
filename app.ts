import { MongoClient, Db, Collection } from "mongodb";
import { faker } from "@faker-js/faker";
import { documentDTO } from "./dto";
import { config } from "./config";
import { setTimeout } from "node:timers/promises";

async function init(): Promise<void> {
  try {
    const client: MongoClient = new MongoClient(config.databaseUrl);
    await client.connect();
    console.log("Connected successfully to server");
    const db: Db = client.db(config.databaseName);
    const collection: Collection = db.collection(config.customerCollectionName);

    while (true) {
      let customersBatch: documentDTO[] = generateCustomers();
      await collection.insertMany(customersBatch);
      console.log(`${customersBatch.length} customers inserted`);
      await setTimeout(200);
    }
  } catch (err) {
    throw new Error(
      `An error occurred: ${err instanceof Error ? err.message : err}`,
    );
  }
}

function generateCustomers(): documentDTO[] {
  let customersBatch: documentDTO[] = [];
  for (let i = 0; i < faker.number.int({ min: 1, max: 10 }); i++) {
    customersBatch.push({
      firstName: faker.person.firstName(),
      lastName: faker.person.lastName(),
      email: faker.internet.email(),
      address: {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state({ abbreviated: true }),
        country: faker.location.countryCode(),
      },
      createdAt: new Date(),
    });
  }
  return customersBatch;
}

init();
