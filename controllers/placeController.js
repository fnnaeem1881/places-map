const { fuzzySearch, indexAllPlacesInElasticsearch,getElasticsearchDataCountServices, fuzzySearchFromMySQL, fuzzySearchFromPostgres } = require('../services/placeService');

// Controller for searching places
exports.searchPlaces = async (req, res) => {
  const { query } = req.query;

  if (!query) {
    return res.status(400).json({ message: 'Query parameter is required' });
  }

  try {
    console.log('Fuzzy search query:', query);
    let elasticSearchResults = await fuzzySearchFromPostgres(query);

    // if (!Array.isArray(elasticSearchResults) || elasticSearchResults.length === 0) {
    //   console.log('No results found or invalid response. Re-indexing...');
    //   await indexAllPlacesInElasticsearch();

    //   // Retry search after re-indexing
    //   elasticSearchResults = await fuzzySearch(query);

    //   if (!Array.isArray(elasticSearchResults) || elasticSearchResults.length === 0) {
    //     return res.status(200).json({ message: 'No results found in Elasticsearch after re-indexing' });
    //   }
    // }

    return res.status(200).json({ results: elasticSearchResults });

  } catch (error) {
    console.error('Error occurred while searching or indexing places:', error);
    return res.status(500).json({ error: 'An error occurred while searching for places' });
  }
};


// Controller for indexing all places from PostgreSQL into Elasticsearch
exports.indexAllPlaces = async (req, res) => {
  try {
    await indexAllPlacesInElasticsearch();
    res.status(200).json({ message: 'All places have been indexed in Elasticsearch' });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'An error occurred while indexing places in Elasticsearch' });
  }
};
// Controller for getting the count of all places in Elasticsearch
exports.getElasticsearchDataCount = async (req, res) => {
    try {
      // Perform a count query to Elasticsearch
     const GetCount=await getElasticsearchDataCountServices();
  
      res.status(200).json({ GetCount });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: 'An error occurred while fetching data count from Elasticsearch' });
    }
  };
  