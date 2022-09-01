import { Handler } from '@netlify/functions';

import { SearchableItem, SearchProjectConfiguration } from "./utils/search-model"
import AlgoliaClient from "./utils/algolia-client";
import KontentClient, { createObjectId } from './utils/kontent-client';
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
  change_type: 'changed' | 'deleted';
  timestamp: string;
}>;

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
    .then(r => tablesClient.upsertEntity({...continuationTokenEntityKeys, token: r.headers[continuationTokenHeaderName]}).then(() => r.data || []));

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

  const changedItemsSubtrees = await Promise.all(changes
    .filter(c => c.change_type === "changed")
    .map(c => loadItemSubtreeFromKontent(c.codename, c.language, config)))

  const deletedObjectIds = changes
    .filter(c => c.change_type === "deleted")
    .map(c => createObjectId(c.codename, c.language));
  const objectIdsChangedToNotIndexable = changedItemsSubtrees
    .filter(notNull)
    .filter(([i]) => !shouldBeIndexed(i, config))
    .map(([i]) => createObjectId(i.system.codename, i.system.language));
  console.log('removing objectIds: ', [...deletedObjectIds, ...objectIdsChangedToNotIndexable]);
  await algoliaClient.removeFromIndex([...deletedObjectIds, ...objectIdsChangedToNotIndexable]);

  const itemsToReindex = ([] as SearchableItem[]).concat(...changedItemsSubtrees
    .filter(notNull)
    .filter(([i]) => shouldBeIndexed(i, config))
    .map(([i, tree]) => createSearchableStructure(i, tree, config)));

  const uniqueItems = Array.from(new Set(itemsToReindex.map(item => item.codename))).map(codename => itemsToReindex.find(item => item.codename === codename));
  console.log('updating objectIds: ', uniqueItems.map(i => i && i.objectID));
  const indexedItems = await algoliaClient.indexSearchableStructure(uniqueItems);

  return {
    statusCode: 200,
    body: `${JSON.stringify(indexedItems)}`,
  };
};

const loadItemSubtreeFromKontent = async (codename: string, language: string, config: SearchProjectConfiguration) => {
  const kontentConfig = { ...config.kontent, language };
  const kontentClient = new KontentClient(kontentConfig, DELIVERY_URL);

  // get all content for requested codename
  const content = await kontentClient.getAllContentForCodename(codename);
  const itemFromDelivery = content.find(item => item.system.codename == codename);
  if (!itemFromDelivery) {
    return null;
  }

  return [itemFromDelivery, content] as const;
}

const shouldBeIndexed = (item: ContentItem, config: SearchProjectConfiguration): boolean =>
  !!item[config.kontent.slugCodename];

const createSearchableStructure = (item: ContentItem, subtree: ContentItem[], config: SearchProjectConfiguration) => {
  const kontentConfig = { ...config.kontent, language: item.system.language };
  const kontentClient = new KontentClient(kontentConfig, DELIVERY_URL);

  return kontentClient.createSearchableStructure([item], subtree);
};

const notNull = <T>(i: T | null): i is T => i !== null;
