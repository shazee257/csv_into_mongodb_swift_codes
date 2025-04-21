const mongoose = require('mongoose');
const csvParser = require('csv-parser');
const fs = require('fs');

// staging
// const MONGO_URI = 'mongodb+srv://anmirza75:Jq4lJKYo2RccCmcL@cluster1.inc5r.mongodb.net/staging_trade_risk_golive?retryWrites=true&w=majority&appName=Cluster1';

// production
const MONGO_URI = 'mongodb+srv://anmirza75:Jq4lJKYo2RccCmcL@cluster1.inc5r.mongodb.net/trade_risk_golive?retryWrites=true&w=majority&appName=Cluster1';


// Define Mongoose Schema
const bankSchema = new mongoose.Schema({
    name: String,
    city: String,
    branch: String,
    swiftCode: String,
    country: String,
    countryFlag: String,
    email: String,
    secondaryEmail: String,
    pocName: String,
    pocPhone: String,
    isActive: { type: Boolean, default: true },
}, { timestamps: true, versionKey: false });

// Create indexes for different search queries
bankSchema.index({ country: 1 });              // For searching by country
bankSchema.index({ country: 1, city: 1 });     // For searching by country and city
bankSchema.index({ country: 1, city: 1, name: 1 }); // For searching by country, city and name
bankSchema.index({ swiftCode: 1 });

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
    // const filePath = 'swift_codes.csv';
    // const filePath = 'staging_demo_pakistan.csv';
    const filePath = 'prod_banks.csv'; // production banks

    fs.createReadStream(filePath)
        .pipe(csvParser({ separator: ',' })) // Changed to comma separator
        .on('data', async (row) => {
            try {
                // Check if we're receiving a single field with all values
                if (row['name,city,swiftCode,country,countryFlag,email,secondaryEmail,pocName,pocPhone']) {
                    // Split the combined value by commas
                    const values = row['name,city,swiftCode,country,countryFlag,email,secondaryEmail,pocName,pocPhone'].split(',');

                    result.push({
                        name: values[0] || '',
                        city: values[1] || '',
                        swiftCode: values[2] || '',
                        country: values[3] || '',
                        countryFlag: values[4] || '',
                        email: values[5] || '',
                        secondaryEmail: values[6] || '',
                        pocName: values[7] || '',
                        pocPhone: values[8] || '',
                        isActive: true
                    });
                } else {
                    // Normal case where fields are already separated
                    result.push({
                        name: row.name || '',
                        city: row.city || '',
                        swiftCode: row.swiftCode || '',
                        country: row.country || '',
                        countryFlag: row.countryFlag || '',
                        email: row.email || '',
                        secondaryEmail: row.secondaryEmail || '',
                        pocName: row.pocName || '',
                        pocPhone: row.pocPhone || '',
                        isActive: true
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
