const amqp = require('amqplib');
const URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
const EXCHANGE = 'events';
const QUEUE = 'notification_payment_queue';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function start() {
  while (true) {
    try {
      const connection = await amqp.connect(URL);
      const channel = await connection.createChannel();
      await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
      await channel.assertQueue(QUEUE, { durable: true });
      await channel.bindQueue(QUEUE, EXCHANGE, 'payment.success');
      channel.prefetch(1);
      console.log(`Notification service waiting on ${QUEUE}`);
      await channel.consume(QUEUE, async msg => {
        if (!msg) return;
        try {
          const event = JSON.parse(msg.content.toString());
          console.log(`NOTIFICATION: payment ${event.status} for order ${event.orderId}`);
          await sleep(1200);
          channel.ack(msg);
        } catch (err) {
          console.error('Notification processing error:', err.message);
          channel.nack(msg, false, false);
        }
      });
      await new Promise(resolve => connection.on('close', resolve));
    } catch (err) {
      console.error('Notification service connection error:', err.message);
      await sleep(3000);
    }
  }
}
start();
