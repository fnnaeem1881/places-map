const { fuzzySearch, indexAllPlacesInElasticsearch,getElasticsearchDataCountServices } = require('../services/placeService');

// Controller for searching places
exports.searchPlaces = async (req, res) => {
  const { query } = req.query;

  if (!query) {
    return res.status(400).json({ message: 'Query parameter is required' });
  }

  try {
    // Perform search in Elasticsearch
    const elasticSearchResults = await fuzzySearch(query);

    if (elasticSearchResults.length === 0) {
      return res.status(200).json({ message: 'No results found in Elasticsearch' });
    }

    return res.status(200).json({ results: elasticSearchResults });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'An error occurred while searching for places' });
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
  