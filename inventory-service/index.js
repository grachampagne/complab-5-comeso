const amqp = require('amqplib');
const URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
const EXCHANGE = 'events';
const QUEUE = 'inventory_order_queue';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function start() {
  while (true) {
    try {
      const connection = await amqp.connect(URL);
      const channel = await connection.createChannel();
      await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
      await channel.assertQueue(QUEUE, { durable: true });
      await channel.bindQueue(QUEUE, EXCHANGE, 'order.placed');
      channel.prefetch(1);
      console.log(`Inventory service waiting on ${QUEUE}`);
      await channel.consume(QUEUE, async msg => {
        if (!msg) return;
        try {
          const order = JSON.parse(msg.content.toString());
          console.log(`Inventory updated for ${order.orderId}: ${order.item} x ${order.quantity}`);
          await sleep(400);
          channel.ack(msg);
        } catch (err) {
          console.error(err);
          channel.nack(msg, false, false);
        }
      });
      await new Promise(resolve => connection.on('close', resolve));
    } catch (err) {
      console.error('Inventory service connection error:', err.message);
      await sleep(3000);
    }
  }
}
start();
