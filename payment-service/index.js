const amqp = require('amqplib');
const URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
const EXCHANGE = 'events';
const QUEUE = 'payment_order_queue';

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
      console.log(`Payment service waiting on ${QUEUE}`);
      await channel.consume(QUEUE, async msg => {
        if (!msg) return;
        try {
          const order = JSON.parse(msg.content.toString());
          console.log('Processing payment for', order.orderId);
          await sleep(700);
          const event = { orderId: order.orderId, status: 'success', paidAt: new Date().toISOString() };
          channel.publish(EXCHANGE, 'payment.success', Buffer.from(JSON.stringify(event)), {
            persistent: true, contentType: 'application/json'
          });
          console.log('Published payment.success:', order.orderId);
          channel.ack(msg);
        } catch (err) {
          console.error('Payment processing error:', err.message);
          channel.nack(msg, false, false);
        }
      });
      connection.on('close', () => console.log('RabbitMQ connection closed; reconnecting'));
      await new Promise(resolve => connection.on('close', resolve));
    } catch (err) {
      console.error('Payment service connection error:', err.message);
      await sleep(3000);
    }
  }
}
start();
