require('dotenv').config(); // Load environment variables from .env file
const express = require('express');
const placeRoutes = require('./routes/placeRoutes');
const cors = require('cors');
const path = require('path');
const cluster = require('cluster');
const os = require('os');

const numCPUs = os.cpus().length;

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);
  console.log(`Forking ${numCPUs} workers...`);

  // Fork workers based on CPU cores
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  // Optional: Restart any worker that exits unexpectedly
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Spawning a new one.`);
    cluster.fork();
  });
} else {
  // Workers share the same TCP connection and run the server

  const app = express();
  app.use(cors());
  app.use(express.json()); // Middleware to parse JSON requests

  // Define routes
  app.use('/api/places', placeRoutes);
  app.get('/', (req, res) => {
    console.log(`Worker ${process.pid} handling request`);
    res.sendFile(path.join(__dirname, './views/index.html'));
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} started and running on port ${PORT}`);
  });
}
