const { Client } = require('@elastic/elasticsearch');
const { Pool } = require('pg');

// Elasticsearch client setup
const client = new Client({ node: process.env.ES_NODE });

// PostgreSQL client setup
const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});


// MYSQL

// const { Client } = require('@elastic/elasticsearch');
// const { Pool } = require('pg');
// const mysql = require('mysql2/promise'); // <--- mysql2 with Promise support
// const fuzz = require('fuzzball');

// // Elasticsearch client setup
// const client = new Client({ node: process.env.ES_NODE });

// // // PostgreSQL client setup
// // const pool = new Pool({
// //     user: process.env.DB_USER,
// //     host: process.env.DB_HOST,
// //     database: process.env.DB_NAME,
// //     password: process.env.DB_PASSWORD,
// //     port: process.env.DB_PORT,
// // });
// const pool = mysql.createPool({
//     host: process.env.DB_HOST,
//     user: process.env.DB_USER,
//     password: process.env.DB_PASSWORD,
//     database: process.env.DB_NAME,
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0,
// });

async function getColumnsFromTable() {
    const [rows] = await pool.query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'places' AND TABLE_SCHEMA = ?", [process.env.DB_NAME]);
    return rows.map(row => row.COLUMN_NAME);
}

// Function to index all data from MySQL to Elasticsearch
exports.indexAllPlacesInElasticsearch = async () => {
    try {
        // Step 1: Fetch column names from the table
        const columns = await getColumnsFromTable();

        // Step 2: Initialize pagination variables
        const batchSize = 1000;
        let offset = 0;

        // Step 3: Loop to fetch data in batches
        let hasMoreData = true;
        while (hasMoreData) {
            // Step 3.1: Fetch data from MySQL (pagination)
            const [rows] = await pool.query(`SELECT * FROM places LIMIT ? OFFSET ?`, [batchSize, offset]);

            if (rows.length === 0) {
                hasMoreData = false;
                break;
            }

            // Step 3.2: Prepare bulk index operations
            const bulkOperations = [];
            rows.forEach((place) => {
                const address = place.address;
                if (!address) {
                    console.warn(`Skipping document because address is missing for place: ${JSON.stringify(place)}`);
                    return;
                }

                bulkOperations.push({
                    update: {
                        _index: 'places',
                        _id: address,
                    },
                });

                const placeData = {};
                columns.forEach((column) => {
                    placeData[column] = place[column];
                });

                bulkOperations.push({
                    doc: placeData,
                    doc_as_upsert: true,
                });
            });

            // Step 3.3: Perform the bulk index operation in Elasticsearch
            if (bulkOperations.length > 0) {
                const response = await client.bulk({ body: bulkOperations });

                if (response.errors) {
                    response.items.forEach((item, index) => {
                        if (item.update && item.update.error) {
                            console.error(`Failed to index document with address: ${rows[index].address}`, item.update.error);
                        }
                    });
                }

                console.log(`Successfully indexed batch starting from offset ${offset}`);
            }

            // Step 3.5: Update the offset
            offset += batchSize;
        }

        console.log('Indexing all places to Elasticsearch completed successfully!');
    } catch (error) {
        console.error('Error indexing places in Elasticsearch:', error);
        throw new Error('Error indexing places in Elasticsearch: ' + error.message);
    }
};
const haversine = (lat1, lon1, lat2, lon2) => {
    const R = 6371e3; // Earth radius in meters
    const φ1 = lat1 * Math.PI / 180;
    const φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180;
    const Δλ = (lon2 - lon1) * Math.PI / 180;

    const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
              Math.cos(φ1) * Math.cos(φ2) *
              Math.sin(Δλ / 2) * Math.sin(Δλ / 2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c; // Distance in meters
};
exports.fuzzySearch = async (query) => {
    try {
        console.log('Fuzzy search query:', query);

        const response = await client.search({
            index: ['places'],
            body: {
                query: {
                    bool: {
                        should: [
                            {
                                match: {
                                    address: {
                                        query: query,
                                        boost: 2,
                                        fuzziness: 'AUTO',
                                    }
                                }
                            },
                            {
                                match_phrase: {
                                    address: {
                                        query: query,
                                        boost: 1.5,
                                    }
                                }
                            },
                            {
                                match: {
                                    address_bn: {
                                        query: query,
                                        boost: 1.2,
                                        fuzziness: 'AUTO',
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        });

        const hits = response.hits?.hits || [];
        if (hits.length === 0) {
            console.log('No results found');
            return [];
        }

        const results = hits.map(hit => hit._source);
        results.sort((a, b) => a.address.length - b.address.length);

        const uniqueResults = [];
        const seenAddresses = new Set();
        const radius = 10;

        results.forEach((hit) => {
            const { lat, long, address } = hit;
            if (!lat || !long) return;

            let isDuplicate = false;
            seenAddresses.forEach(existing => {
                const [existingLat, existingLon] = existing.split(',');
                const distance = haversine(lat, long, parseFloat(existingLat), parseFloat(existingLon));
                if (distance <= radius) {
                    isDuplicate = true;
                }
            });

            if (!isDuplicate) {
                uniqueResults.push(hit);
                seenAddresses.add(`${lat},${long}`);
            }
        });

        return uniqueResults;

    } catch (error) {
        console.error('Error performing Elasticsearch search:', error);
        return []; // 👈 This prevents controller crashes
    }
};

async function checkAndEnablePgTrgm() {
    try {
        // Check if the pg_trgm extension is installed
        const result = await pool.query(`
            SELECT 1 
            FROM pg_extension 
            WHERE extname = 'pg_trgm';
        `);

        // If pg_trgm is not installed, run CREATE EXTENSION
        if (result.rowCount === 0) {
            console.log('pg_trgm not found, creating extension...');
            await pool.query('CREATE EXTENSION IF NOT EXISTS pg_trgm');
            console.log('pg_trgm extension created successfully!');
        } else {
            console.log('pg_trgm extension is already installed.');
        }
    } catch (error) {
        console.error('Error checking or creating pg_trgm extension:', error);
    }
}


// Run the check and enable pg_trgm extension
checkAndEnablePgTrgm();
exports.fuzzySearchFromPostgres = async (query) => {
    try {
        // SQL query for fuzzy search without limiting DB results
        const sql = `
            SELECT * 
            FROM places
            WHERE address % $1 OR address_bn % $1
            ORDER BY similarity(address, $1) DESC, LENGTH(address) LIMIT 10;
        `;
        const { rows } = await pool.query(sql, [query]);

        const uniqueResults = [];

        rows.forEach(place => {
            const existingPlace = uniqueResults.find(p => 
                (p.address === place.address || p.address_bn === place.address_bn) ||
                haversine(p.lat, p.long, place.lat, place.long) <= 10
            );

            if (!existingPlace) {
                uniqueResults.push(place);
            }
        });
        console.log('Unique results:', uniqueResults.length);
        // Sort by address length (shortest first)
        // uniqueResults.sort((a, b) => a.address.length - b.address.length);
        return uniqueResults; 

        // Return only the top 10 filtered results
        return uniqueResults.slice(0, 10);

    } catch (error) {
        console.error('Postgres fuzzy search error:', error);
        return [];
    }
};


exports.fuzzySearchFromMySQL = async (query) => {
    try {
        const [rows] = await pool.query(
            'SELECT id, address, address_bn, lat, `long` FROM places WHERE address IS NOT NULL'
        );

        const resultsWithScore = rows.map(place => {
            const score = fuzz.token_set_ratio(query, place.address || '');
            return { ...place, _score: score };
        });

        // Filter by score threshold
        const filtered = resultsWithScore.filter(r => r._score >= 70);

        // Deduplicate by lowercase address
        const addressMap = new Map();
        for (const place of filtered) {
            const key = (place.address || '').toLowerCase().trim();

            if (!addressMap.has(key)) {
                addressMap.set(key, place);
            } else {
                const existing = addressMap.get(key);
                // Prefer higher score, then shorter address
                if (
                    place._score > existing._score ||
                    (place._score === existing._score && place.address.length < existing.address.length)
                ) {
                    addressMap.set(key, place);
                }
            }
        }

        // Sort and limit to top 5
        const sortedUnique = [...addressMap.values()]
            .sort((a, b) => b._score - a._score || a.address.length - b.address.length)
            .slice(0, 5);

        return sortedUnique;

    } catch (error) {
        console.error('MySQL fuzzy search error:', error);
        return [];
    }
};

// Function to get data count from Elasticsearch
exports.getElasticsearchDataCountServices = async () => {
    try {
        // Perform a count query to Elasticsearch
        const countResponse = await client.count({
            index: ['places_a', 'places'], // <-- search both indexes
        });

        // Log the full response to inspect its structure
        console.log('Elasticsearch Response:', countResponse);

        // Access count directly from countResponse or countResponse.body
        const count = countResponse.body && countResponse.body.count !== undefined
            ? countResponse.body.count
            : countResponse.count;

        // Ensure the count is valid
        if (count !== undefined) {
            return count;
        } else {
            throw new Error('Count field is missing in the Elasticsearch response');
        }
    } catch (error) {
        console.error(error);
        throw new Error('An error occurred while fetching data count from Elasticsearch');
    }
};

