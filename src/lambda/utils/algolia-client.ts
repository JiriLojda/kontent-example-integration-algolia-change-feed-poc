import { AlgoliaConfiguration, SearchableItem } from "./search-model"
import AlgoliaSearch, { SearchClient, SearchIndex } from 'algoliasearch'


class AlgoliaClient {
  config: AlgoliaConfiguration;
  index: SearchIndex;

  constructor(config: AlgoliaConfiguration) {
    this.config = config;

    // init the algolia client & index
    const algoliaClient: SearchClient = AlgoliaSearch(this.config.appId, this.config.apiKey);
    this.index = algoliaClient.initIndex(this.config.index);
    // set search settings to only include processed content fields
  }

  // setup index
  async setupIndex() {
    let result = await this.index.setSettings({
      searchableAttributes: ["content.contents", "content.name", "name"],
      attributesForFaceting: ["content.codename", "language"],
      attributesToSnippet: ['content.contents:80']
    }).wait();
  }
  
  // indexes searchable structure of content into algolia
  async indexSearchableStructure(structure: SearchableItem[] | any): Promise<string[]> {
    // push searchable objects into algolia
    const indexed = await (this.index.saveObjects(structure).wait());
    return indexed.objectIDs;
  }

  // returns the indexed content item(s) that include searched content item
  async searchIndex(searchedCodename: string, language: string): Promise<SearchableItem[]> {
    try {
      const response = await this.index.search<SearchableItem>("", {
        facetFilters: [`content.codename: ${searchedCodename}`, `content.language: ${language}`]
      });
      return response.hits;
    }
    catch (error) {
      return [];
    }
  }

  // removes items from the index
  async removeFromIndex(objectIds: string[]): Promise<string[]> {
    try {
      const response = await this.index.deleteObjects(objectIds).wait();
      return response.objectIDs;
    }
    catch (error) {
      return [];
    }
  }

}

export default AlgoliaClient;
