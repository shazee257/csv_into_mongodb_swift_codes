const mongoose = require('mongoose');
const csvParser = require('csv-parser');
const fs = require('fs');

// MongoDB Connection String
const MONGO_URI = 'mongodb://localhost:27017/bankdb';

// Define Mongoose Schema
const bankSchema = new mongoose.Schema({
    bank: String,
    city: String,
    branch: String,
    swiftCode: String,
    country: String
}, { versionKey: false });

// Create indexes for different search queries
bankSchema.index({ country: 1 });              // For searching by country
bankSchema.index({ country: 1, city: 1 });     // For searching by country and city
bankSchema.index({ country: 1, city: 1, bank: 1 }); // For searching by country, city and bank

const Bank = mongoose.model('Bank', bankSchema);

// Connect to MongoDB
mongoose.connect(MONGO_URI)
    .then(() => {
        console.log('Connected to MongoDB');

        // Only process CSV after successful connection
        processCSV();
    })
    .catch(err => console.error('MongoDB Connection Error:', err));

// Function to process CSV data
function processCSV() {
    // each data array of object having bank, city, branch, swiftCode, country keys
    const result = [];
    const filePath = 'swift_codes.csv';

    fs.createReadStream(filePath)
        .pipe(csvParser({ separator: ',' })) // Changed to comma separator
        .on('data', async (row) => {
            try {
                // Check if we're receiving a single field with all values
                if (row['bank,city,branch,swiftCode,country']) {
                    // Split the combined value by commas
                    const values = row['bank,city,branch,swiftCode,country'].split(',');

                    result.push({
                        bank: values[0] || '',
                        city: values[1] || '',
                        branch: values[2] || '',
                        swiftCode: values[3] || '',
                        country: values[4] || ''
                    });
                } else {
                    // Normal case where fields are already separated
                    result.push({
                        bank: row.bank || '',
                        city: row.city || '',
                        branch: row.branch || '',
                        swiftCode: row.swiftCode || '',
                        country: row.country || ''
                    });
                }
                console.log('Processed row');
            } catch (error) {
                console.error('Error processing row:', error, row);
            }
        })
        .on('end', () => {
            console.log('CSV file successfully processed');
            console.log(`Total records: ${result.length}`);

            // Display first few records as sample
            console.log("Sample data:", result.slice(0, 6));

            // You could also save the data to MongoDB here if needed
            Bank.insertMany(result)
                .then(() => console.log('All data inserted to MongoDB'))
                .catch(err => console.error('Error inserting data:', err))
                .finally(() => mongoose.connection.close());
        });
}
