const express = require('express');
const amqp = require('amqplib');
const app = express();
app.use(express.json());

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
const EXCHANGE = 'events';
let channel;

async function connectRabbit() {
  while (!channel) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();
      await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
      console.log('Order service connected to RabbitMQ');
      connection.on('close', () => { channel = null; setTimeout(connectRabbit, 3000); });
    } catch (err) {
      console.error('RabbitMQ unavailable; retrying in 3 seconds:', err.message);
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }
}
connectRabbit();

app.get('/health', (_req, res) => res.json({ service: 'order-service', status: 'ok' }));
app.post('/orders', async (req, res) => {
  const { customer, item, quantity } = req.body || {};
  if (!customer || !item || !Number.isInteger(Number(quantity)) || Number(quantity) < 1) {
    return res.status(400).json({ error: 'Provide customer, item, and quantity of at least 1.' });
  }
  if (!channel) return res.status(503).json({ error: 'Message broker is not ready. Try again.' });
  const order = {
    orderId: `ORD-${Date.now()}`,
    customer: String(customer),
    item: String(item),
    quantity: Number(quantity),
    createdAt: new Date().toISOString()
  };
  try {
    channel.publish(EXCHANGE, 'order.placed', Buffer.from(JSON.stringify(order)), {
      persistent: true, contentType: 'application/json'
    });
    console.log('Published order.placed:', order.orderId);
    res.status(201).json({ message: 'Order placed and order.placed event published.', order });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Could not publish order event.' });
  }
});
app.listen(process.env.PORT || 4001, () => console.log('Order service listening on 4001'));
