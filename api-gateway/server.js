const express = require('express');
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ service: 'api-gateway', status: 'ok' }));

app.post('/orders', async (req, res) => {
  try {
    const upstream = process.env.ORDER_SERVICE_URL || 'http://localhost:4001';
    const response = await fetch(`${upstream}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body)
    });
    const body = await response.json();
    res.status(response.status).json(body);
  } catch (err) {
    console.error('Order service request failed:', err.message);
    res.status(502).json({ error: 'Cannot reach order service. Check Docker logs.' });
  }
});

const port = process.env.PORT || 4000;
app.listen(port, () => console.log(`API Gateway listening on ${port}`));
