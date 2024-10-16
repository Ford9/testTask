import { ObjectId } from "mongodb";

export type documentDTO = {
  _id?: ObjectId;
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  };
  createdAt: Date;
};

export type batchDTO = {
  replaceOne: {
    filter: { _id: ObjectId | undefined };
    replacement: documentDTO;
    upsert: boolean;
  };
}[];
