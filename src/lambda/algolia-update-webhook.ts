import { APIGatewayEvent, APIGatewayProxyEventQueryStringParameters, Context } from 'aws-lambda'
import { Handler, schedule } from '@netlify/functions';

import { IWebhookDeliveryResponse, IWebhookDeliveryItem, SignatureHelper } from "@kentico/kontent-webhook-helper";

import { SearchableItem, SearchProjectConfiguration } from "./utils/search-model"
import AlgoliaClient from "./utils/algolia-client";
import KontentClient from './utils/kontent-client';
import { ContentItem } from '@kentico/kontent-delivery';
import { TableClient } from "@azure/data-tables";
import axios from "axios";

// @ts-ignore - netlify env. variable
const { ALGOLIA_API_KEY, PROJECT_ID, CONTINUATION_TOKENS_CONNECTION_STRING, CONTINUATION_TOKENS_TABLE_NAME, ALGOLIA_APP_ID, ALGOLIA_INDEX_NAME, URL_SLUG, DELIVERY_URL } = process.env;

type ChangeFeedItem = Readonly<{
  project_id: string;
  codename: string;
  id: string;
  type: string;
  language: string;
  collection: string;
  change_type: string;
  timestamp: string;
}>;

// processes affected content (about which we have been notified by the webhook)
async function processNotIndexedContent(codename: string, language: string, config: SearchProjectConfiguration) {
  const kontentConfig = config.kontent;
  kontentConfig.language = language;
  const kontentClient = new KontentClient(kontentConfig, DELIVERY_URL);

  // get all content for requested codename
  const content: ContentItem[] = await kontentClient.getAllContentForCodename(codename);
  const itemFromDelivery = content.find(item => item.system.codename == codename);

  // the item has slug => new record
  if (itemFromDelivery && itemFromDelivery[config.kontent.slugCodename]) {
    // creates a searchable structure based on the content's structure
    const searchableStructure = kontentClient.createSearchableStructure([itemFromDelivery], content);
    return searchableStructure;
  }

  return [];
}

// processes affected content (about which we have been notified by the webhook)
async function processIndexedContent(codename: string, language: string, config: SearchProjectConfiguration, algoliaClient: AlgoliaClient) {
  const kontentConfig = config.kontent;
  kontentConfig.language = language;
  const kontentClient = new KontentClient(kontentConfig);

  // get all content for requested codename
  const content: ContentItem[] = await kontentClient.getAllContentForCodename(codename);
  const itemFromDelivery = content.find(item => item.system.codename == codename);

  // nothing found in Kontent => item has been removed
  if (!itemFromDelivery) {
    await algoliaClient.removeFromIndex([codename]);
    return [];
  }

  // some content has been found => update existing item by processing it once again
  const searchableStructure = kontentClient.createSearchableStructure([itemFromDelivery], content);
  return searchableStructure;
}
const tablesClient = TableClient.fromConnectionString(CONTINUATION_TOKENS_CONNECTION_STRING, CONTINUATION_TOKENS_TABLE_NAME);
const continuationTokenEntityKeys = {
  partitionKey: 'algolia',
  rowKey: 'continuationToken',
};
const continuationTokenHeaderName = 'x-continuation';

/* FUNCTION HANDLER */
export const handler: Handler = async (event, context) => {
  const changeFeedUrl = `${DELIVERY_URL}/${PROJECT_ID}/change-feed`;


  const continuationToken = await tablesClient.getEntity(continuationTokenEntityKeys.partitionKey, continuationTokenEntityKeys.rowKey)
    .then(r => r.token || Promise.reject())
    .catch(() => axios.get(changeFeedUrl).then(r => {
      console.log('feed response: ', r);
      return r.headers[continuationTokenHeaderName];
    })) as string | null;

  if (!continuationToken) {
    throw new Error('No token is stored and change feed did not return token.');
  }

  const changes: ReadonlyArray<ChangeFeedItem> = await axios.get(changeFeedUrl, { headers: { [continuationTokenHeaderName]: continuationToken } })
    .then(r => tablesClient.upsertEntity({ ...continuationTokenEntityKeys, token: r.headers[continuationTokenHeaderName] }).then(() => r.data));

  // create configuration from the webhook body/query params
  const config = {
    kontent: {
      projectId: PROJECT_ID,
      slugCodename: URL_SLUG,
    },
    algolia: {
      appId: ALGOLIA_APP_ID,
      apiKey: ALGOLIA_API_KEY,
      index: ALGOLIA_INDEX_NAME,
    }
  };

  if (!config) {
    return { statusCode: 400, body: "Missing Parameters" };
  }

  const algoliaClient = new AlgoliaClient(config.algolia);
  const itemsToIndex: SearchableItem[] = [];

  // go through updated items
  for (const affectedItem of changes) {
    // we are looking for the ultimate "parent"/indexed item that contains the content that has been updated

    // found an item in algolia
    const foundItems: SearchableItem[] = await algoliaClient.searchIndex(affectedItem.codename, affectedItem.language);

    // item not found in algolia  => new content to be indexed?
    if (foundItems.length == 0) {
      itemsToIndex.push(...await processNotIndexedContent(affectedItem.codename, affectedItem.language, config));
    }

    // we actually found some items in algolia => update or delete?
    for (const foundItem of foundItems) {
      itemsToIndex.push(...await processIndexedContent(foundItem.codename, foundItem.language, config, algoliaClient));
    }
  }

  const uniqueItems = Array.from(new Set(itemsToIndex.map(item => item.codename))).map(codename => { return itemsToIndex.find(item => item.codename === codename) });
  const indexedItems: string[] = await algoliaClient.indexSearchableStructure(uniqueItems);

  return {
    statusCode: 200,
    body: `${JSON.stringify(indexedItems)}`,
  };
};
//
// export const handler = schedule('0-59 * * * *', handlerWithoutSchedule);
