import {
  MongoClient,
  Db,
  Collection,
  ChangeStreamDocument,
  ResumeToken,
} from "mongodb";
import crypto from "crypto";
import { documentDTO, batchDTO } from "./dto";
import { config } from "./config";

async function init(): Promise<void> {
  const client: MongoClient = new MongoClient(config.databaseUrl);
  const fullReindex: boolean = process.argv.includes("--full-reindex");
  await client.connect();
  console.log("Connected to MongoDB");

  const db: Db = client.db(config.databaseName);
  const sourceCollection: Collection<documentDTO> = db.collection(
    config.customerCollectionName,
  );
  const targetCollection: Collection<documentDTO> = db.collection(
    config.anonimCustomersCollectionName,
  );

  if (fullReindex) {
    await doFullReindex(sourceCollection, targetCollection);
    await client.close();
  } else {
    const configCollection: Collection = db.collection(
      config.configCollectionName,
    );
    listenChanges(sourceCollection, targetCollection, configCollection);
  }
}

async function doFullReindex(
  sourceCollection: Collection<documentDTO>,
  targetCollection: Collection<documentDTO>,
): Promise<void> {
  const cursor = sourceCollection
    .find({})
    .sort({ firstName: 1 })
    .batchSize(config.collectionBatchSize);
  let batch: batchDTO = [];

  for await (const document of cursor) {
    const anonymizedDocument: documentDTO = anonymizeDocument(document);
    batch.push({
      replaceOne: {
        filter: { _id: anonymizedDocument._id },
        replacement: anonymizedDocument,
        upsert: true,
      },
    });

    if (batch.length >= 1000) {
      await targetCollection.bulkWrite(batch);
      batch = [];
      console.log("1000 documents processed");
    }
  }

  if (batch.length > 0) {
    await targetCollection.bulkWrite(batch);
    console.log(`${batch.length} documents processed`);
  }

  console.log("Full reindex completed");
}

function anonymizeString(inputString: string): string {
  // Генерируем хэш от входной строки
  const hash: string = crypto
    .createHash("sha256")
    .update(inputString)
    .digest("hex");
  let result: string = "";
  const alphabet: string =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  const base: number = alphabet.length;

  // Преобразуем хэш в детерминированную последовательность символов
  for (let i = 0; i < 8; i++) {
    // Преобразуем каждые два символа хэша в число и используем его для выбора символа из алфавита
    const segment: string = hash.substring(i * 2, i * 2 + 2);
    const index: number = parseInt(segment, 16) % base;
    result += alphabet[index];
  }

  return result;
}

function anonymizeDocument(document: any): documentDTO {
  let splitedEmail: string[] = document.email.split("@");
  let anonymizeEmail: string = `${anonymizeString(splitedEmail[0])}@${splitedEmail[1]}`;
  return {
    ...document,
    firstName: anonymizeString(document.firstName),
    lastName: anonymizeString(document.lastName),
    email: anonymizeEmail,
    address: {
      ...document.address,
      line1: anonymizeString(document.address.line1),
      line2: anonymizeString(document.address.line2),
      postcode: anonymizeString(document.address.postcode),
    },
  };
}

async function listenChanges(
  sourceCollection: Collection<documentDTO>,
  targetCollection: Collection<documentDTO>,
  configCollection: Collection,
): Promise<void> {
  let batch: batchDTO = [];
  let batchTimer: NodeJS.Timeout | null = null;

  const addDocumentToBatch = async (document: documentDTO): Promise<void> => {
    batch.push({
      replaceOne: {
        filter: { _id: document._id },
        replacement: document,
        upsert: true,
      },
    });
    if (batch.length >= 1000) {
      await processBatch();
    } else if (!batchTimer) {
      batchTimer = setTimeout(async () => {
        await processBatch();
      }, 1000);
    }
  };

  const processBatch = async (): Promise<void> => {
    if (batch.length > 0) {
      await targetCollection.bulkWrite(batch);
      console.log(`${batch.length} documents processed and inserted`);
      batch = [];
    }
    if (batchTimer) {
      clearTimeout(batchTimer);
      batchTimer = null;
    }
  };

  const saveResumeToken = async (
    resumeTokenName: string,
    token: ResumeToken,
  ): Promise<void> => {
    await configCollection.updateOne(
      { name: resumeTokenName },
      { $set: { token } },
      { upsert: true },
    );
  };

  const getResumeToken = async (
    resumeTokenName: string,
  ): Promise<ChangeStreamDocument | undefined> => {
    const tokenDoc = await configCollection.findOne({
      name: resumeTokenName,
    });
    return tokenDoc ? tokenDoc.token : undefined;
  };

  let resumeTokenName: string =
    "resumeTokenFor " + config.customerCollectionName;
  let resumeToken: ChangeStreamDocument | undefined =
    await getResumeToken(resumeTokenName);
  let changeStreamOptions = {
    fullDocument: "updateLookup",
    ...(resumeToken ? { resumeAfter: resumeToken } : {}),
  };
  const pipeline = [
    {
      $match: {
        operationType: { $in: ["insert", "update"] },
      },
    },
  ];
  const changeStream = sourceCollection
    .watch(pipeline, changeStreamOptions)
    .stream();

  for await (const change of changeStream) {
    if ("fullDocument" in change && change.fullDocument) {
      const anonymizedDocument: documentDTO = anonymizeDocument(
        change.fullDocument,
      );
      addDocumentToBatch(anonymizedDocument);
    }

    await saveResumeToken(resumeTokenName, change._id);
  }
}

init();
