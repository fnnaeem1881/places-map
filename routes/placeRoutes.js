const express = require('express');
const router = express.Router();
const placeController = require('../controllers/placeController');

// Route to search for places using Elasticsearch
router.get('/search', placeController.searchPlaces);

// Route to index all places from PostgreSQL into Elasticsearch
router.post('/index-all-places', placeController.indexAllPlaces);
router.get('/elasticsearch-count', placeController.getElasticsearchDataCount);

module.exports = router;
