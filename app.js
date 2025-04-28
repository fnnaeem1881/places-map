require('dotenv').config(); // Load environment variables from .env file
const express = require('express');
const placeRoutes = require('./routes/placeRoutes');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());

app.use(express.json()); // Middleware to parse JSON requests

// Define routes
app.use('/api/places', placeRoutes);
app.get('/', (req, res) => {
  console.log('index route hit');
  
  res.sendFile(path.join(__dirname, './views/index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
