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

// Function to fetch column names from the PostgreSQL table
async function getColumnsFromTable() {
    const res = await pool.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'places_a'");
    return res.rows.map(row => row.column_name);
}

// Function to index all data from PostgreSQL to Elasticsearch
exports.indexAllPlacesInElasticsearch = async () => {
    try {
        // Step 1: Fetch column names from the table
        const columns = await getColumnsFromTable();

        // Step 2: Initialize pagination variables
        const batchSize = 1000; // Size of each batch for processing
        let offset = 0;

        // Step 3: Loop to fetch data in batches
        let hasMoreData = true;
        while (hasMoreData) {
            // Step 3.1: Fetch data from PostgreSQL (pagination)
            const query = `SELECT * FROM places_a LIMIT ${batchSize} OFFSET ${offset}`;
            const res = await pool.query(query);

            if (res.rows.length === 0) {
                hasMoreData = false; // No more data to fetch
                break;
            }

            // Step 3.2: Prepare bulk index operations
            const bulkOperations = [];
            res.rows.forEach((place) => {
                const address = place.address; // Use address as the unique identifier
                if (!address) {
                    console.warn(`Skipping document because address is missing for place: ${JSON.stringify(place)}`);
                    return; // Skip places without an address
                }

                // Step 3.2.1: Check if a document with the same address already exists in Elasticsearch
                bulkOperations.push({
                    update: {
                        _index: 'places_a',
                        _id: address, // Use address as the document ID to ensure uniqueness
                    },
                });

                // Prepare the document to be indexed (or updated if exists)
                const placeData = {};
                columns.forEach((column) => {
                    placeData[column] = place[column]; // Add each column and its value to the document
                });

                bulkOperations.push({
                    doc: placeData, // Update the document
                    doc_as_upsert: true, // If document doesn't exist, insert it
                });
            });

            // Step 3.3: Perform the bulk index operation in batches
            if (bulkOperations.length > 0) {
                const response = await client.bulk({ body: bulkOperations });
                
                // Step 3.4: Handle partial failures in Elasticsearch bulk response
                if (response.errors) {
                    response.items.forEach((item, index) => {
                        if (item.update && item.update.error) {
                            console.error(`Failed to index document with address: ${res.rows[index].address}`, item.update.error);
                        }
                    });
                }

                console.log(`Successfully indexed batch starting from offset ${offset}`);
            }

            // Step 3.5: Update the offset for the next batch
            offset += batchSize;
        }

        console.log('Indexing all places to Elasticsearch completed successfully!');
    } catch (error) {
        console.error('Error indexing places in Elasticsearch:', error);
        throw new Error('Error indexing places in Elasticsearch: ' + error.message);
    }
};

exports.fuzzySearch = async (query) => {
    try {
        console.log('Fuzzy search query:', query); // Log the query to check its value

        const response = await client.search({
            index: 'places_a',
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

        // Extract the unique locations based on the 'address' field
        const uniqueResults = [];
        const seenAddresses = new Set();

        hits.forEach((hit) => {
            const address = hit._source.address;
            if (!seenAddresses.has(address)) {
                seenAddresses.add(address);
                uniqueResults.push(hit._source); // Add unique location to the results array
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
            index: 'places_a', // Ensure the index name is correct
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
