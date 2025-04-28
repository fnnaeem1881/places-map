// const { Client } = require('@elastic/elasticsearch');
// const { Pool } = require('pg');

// // Elasticsearch client setup
// const client = new Client({ node: process.env.ES_NODE });

// // PostgreSQL client setup
// const pool = new Pool({
//     user: process.env.DB_USER,
//     host: process.env.DB_HOST,
//     database: process.env.DB_NAME,
//     password: process.env.DB_PASSWORD,
//     port: process.env.DB_PORT,
// });

// // Function to fetch column names from the PostgreSQL table
// async function getColumnsFromTable() {
//     const res = await pool.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'places_a'");
//     return res.rows.map(row => row.column_name);
// }

// // Function to index all data from PostgreSQL to Elasticsearch
// exports.indexAllPlacesInElasticsearch = async () => {
//     try {
//         // Step 1: Fetch column names from the table
//         const columns = await getColumnsFromTable();

//         // Step 2: Initialize pagination variables
//         const batchSize = 1000; // Size of each batch for processing
//         let offset = 0;

//         // Step 3: Loop to fetch data in batches
//         let hasMoreData = true;
//         while (hasMoreData) {
//             // Step 3.1: Fetch data from PostgreSQL (pagination)
//             const query = `SELECT * FROM places_b LIMIT ${batchSize} OFFSET ${offset}`;
//             const res = await pool.query(query);

//             if (res.rows.length === 0) {
//                 hasMoreData = false; // No more data to fetch
//                 break;
//             }

//             // Step 3.2: Prepare bulk index operations
//             const bulkOperations = [];
//             res.rows.forEach((place) => {
//                 const address = place.address; // Use address as the unique identifier
//                 if (!address) {
//                     console.warn(`Skipping document because address is missing for place: ${JSON.stringify(place)}`);
//                     return; // Skip places without an address
//                 }

//                 // Step 3.2.1: Check if a document with the same address already exists in Elasticsearch
//                 bulkOperations.push({
//                     update: {
//                         _index: 'places',
//                         _id: address, // Use address as the document ID to ensure uniqueness
//                     },
//                 });

//                 // Prepare the document to be indexed (or updated if exists)
//                 const placeData = {};
//                 columns.forEach((column) => {
//                     placeData[column] = place[column]; // Add each column and its value to the document
//                 });

//                 bulkOperations.push({
//                     doc: placeData, // Update the document
//                     doc_as_upsert: true, // If document doesn't exist, insert it
//                 });
//             });

//             // Step 3.3: Perform the bulk index operation in batches
//             if (bulkOperations.length > 0) {
//                 const response = await client.bulk({ body: bulkOperations });
                
//                 // Step 3.4: Handle partial failures in Elasticsearch bulk response
//                 if (response.errors) {
//                     response.items.forEach((item, index) => {
//                         if (item.update && item.update.error) {
//                             console.error(`Failed to index document with address: ${res.rows[index].address}`, item.update.error);
//                         }
//                     });
//                 }

//                 console.log(`Successfully indexed batch starting from offset ${offset}`);
//             }

//             // Step 3.5: Update the offset for the next batch
//             offset += batchSize;
//         }

//         console.log('Indexing all places to Elasticsearch completed successfully!');
//     } catch (error) {
//         console.error('Error indexing places in Elasticsearch:', error);
//         throw new Error('Error indexing places in Elasticsearch: ' + error.message);
//     }
// };
// const haversine = (lat1, lon1, lat2, lon2) => {
//     const R = 6371e3; // Earth radius in meters
//     const φ1 = lat1 * Math.PI / 180;
//     const φ2 = lat2 * Math.PI / 180;
//     const Δφ = (lat2 - lat1) * Math.PI / 180;
//     const Δλ = (lon2 - lon1) * Math.PI / 180;

//     const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
//               Math.cos(φ1) * Math.cos(φ2) *
//               Math.sin(Δλ / 2) * Math.sin(Δλ / 2);

//     const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
//     return R * c; // Distance in meters
// };

// exports.fuzzySearch = async (query) => {
//     try {
//         console.log('Fuzzy search query:', query);

//         const response = await client.search({
//             index: 'places_a',
//             body: {
//                 query: {
//                     bool: {
//                         should: [
//                             {
//                                 match: {
//                                     address: {
//                                         query: query,
//                                         boost: 2,  // Boost exact matches for better relevance
//                                         fuzziness: 'AUTO',
//                                     }
//                                 }
//                             },
//                             {
//                                 match_phrase: {
//                                     address: {
//                                         query: query,
//                                         boost: 1.5,  // Boost phrase match slightly less
//                                     }
//                                 }
//                             },
//                             {
//                                 match: {
//                                     address_bn: {
//                                         query: query,
//                                         boost: 1.2, // You can also apply similar boosting to Bengali address field
//                                         fuzziness: 'AUTO',
//                                     }
//                                 }
//                             }
//                         ]
//                     }
//                 }
//             }
//         });

//         const hits = response.hits?.hits || [];
//         if (hits.length === 0) {
//             console.log('No results found');
//             return { results: [] };
//         }

//         // Extract the results
//         const results = hits.map(hit => hit._source);

//         // Sort by address length (shortest first)
//         results.sort((a, b) => a.address.length - b.address.length);

//         const uniqueResults = [];
//         const seenAddresses = new Set();
//         const radius = 10; // 10 meters

//         results.forEach((hit) => {
//             const { lat, long, address } = hit;
//             let isDuplicate = false;

//             // Skip if lat/long is missing
//             if (!lat || !long) return;

//             // Check if this address is too close (within 10 meters) to any already seen address
//             seenAddresses.forEach(existing => {
//                 const [existingLat, existingLon] = existing.split(',');
//                 const distance = haversine(lat, long, parseFloat(existingLat), parseFloat(existingLon));
//                 if (distance <= radius) {
//                     isDuplicate = true;  // Found a duplicate within 10 meters
//                 }
//             });

//             // If it's not a duplicate, add to results
//             if (!isDuplicate) {
//                 uniqueResults.push(hit);
//                 seenAddresses.add(`${lat},${long}`); // Add lat, lon as a unique key for comparison
//             }
//         });

//         return uniqueResults;

//     } catch (error) {
//         console.error('Error performing Elasticsearch search:', error);
//         throw new Error('Error performing Elasticsearch search: ' + error.message);
//     }
// };


// // Function to get data count from Elasticsearch
// exports.getElasticsearchDataCountServices = async () => {
//     try {
//         // Perform a count query to Elasticsearch
//         const countResponse = await client.count({
//             index: 'places_a', // Ensure the index name is correct
//         });

//         // Log the full response to inspect its structure
//         console.log('Elasticsearch Response:', countResponse);

//         // Access count directly from countResponse or countResponse.body
//         const count = countResponse.body && countResponse.body.count !== undefined
//             ? countResponse.body.count
//             : countResponse.count;

//         // Ensure the count is valid
//         if (count !== undefined) {
//             return count;
//         } else {
//             throw new Error('Count field is missing in the Elasticsearch response');
//         }
//     } catch (error) {
//         console.error(error);
//         throw new Error('An error occurred while fetching data count from Elasticsearch');
//     }
// };


// MYSQL

const { Client } = require('@elastic/elasticsearch');
const { Pool } = require('pg');
const mysql = require('mysql2/promise'); // <--- mysql2 with Promise support

// Elasticsearch client setup
const client = new Client({ node: process.env.ES_NODE });

// // PostgreSQL client setup
// const pool = new Pool({
//     user: process.env.DB_USER,
//     host: process.env.DB_HOST,
//     database: process.env.DB_NAME,
//     password: process.env.DB_PASSWORD,
//     port: process.env.DB_PORT,
// });
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

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
            index: ['places_a', 'places'], // <-- search both indexes
            body: {
                query: {
                    bool: {
                        should: [
                            {
                                match: {
                                    address: {
                                        query: query,
                                        boost: 2,  // Boost exact matches for better relevance
                                        fuzziness: 'AUTO',
                                    }
                                }
                            },
                            {
                                match_phrase: {
                                    address: {
                                        query: query,
                                        boost: 1.5,  // Boost phrase match slightly less
                                    }
                                }
                            },
                            {
                                match: {
                                    address_bn: {
                                        query: query,
                                        boost: 1.2, // You can also apply similar boosting to Bengali address field
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
            return { results: [] };
        }

        // Extract the results
        const results = hits.map(hit => hit._source);

        // Sort by address length (shortest first)
        results.sort((a, b) => a.address.length - b.address.length);

        const uniqueResults = [];
        const seenAddresses = new Set();
        const radius = 10; // 10 meters

        results.forEach((hit) => {
            const { lat, long, address } = hit;
            let isDuplicate = false;

            // Skip if lat/long is missing
            if (!lat || !long) return;

            // Check if this address is too close (within 10 meters) to any already seen address
            seenAddresses.forEach(existing => {
                const [existingLat, existingLon] = existing.split(',');
                const distance = haversine(lat, long, parseFloat(existingLat), parseFloat(existingLon));
                if (distance <= radius) {
                    isDuplicate = true;  // Found a duplicate within 10 meters
                }
            });

            // If it's not a duplicate, add to results
            if (!isDuplicate) {
                uniqueResults.push(hit);
                seenAddresses.add(`${lat},${long}`); // Add lat, lon as a unique key for comparison
            }
        });

        return uniqueResults;

    } catch (error) {
        console.error('Error performing Elasticsearch search:', error);
        throw new Error('Error performing Elasticsearch search: ' + error.message);
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

