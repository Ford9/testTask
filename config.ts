import "dotenv/config";
import Joi from "joi";

const envVarsSchema = Joi.object({
  DB_URI: Joi.string().required(),
}).required();

const { value: envVars, error } = envVarsSchema
  .prefs({ errors: { label: "key" } })
  .unknown(true)
  .validate(process.env);

if (error) {
  throw new Error(
    `Error in environment variable configuration: ${error.message}`,
  );
}

export const config = {
  databaseUrl: envVars.DB_URI,
  databaseName: "anonymizationService",
  customerCollectionName: "customers",
  anonimCustomersCollectionName: "customers_anonymised",
  configCollectionName: "config",
  collectionBatchSize: 100,
};
