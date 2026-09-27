const amqp = require('amqplib');

async function runConsumerA() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'payments.event';
    const queueName = 'q_payment_success';

    await channel.assertExchange(exchangeName, 'topic', { durable: true });
    // Membuat antrean yang tidak hilang saat restart (durable)
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, exchangeName, 'payment.success');
    
    // Jangan proses pesan secara bersamaan terlalu banyak
    channel.prefetch(1);
    console.log(" [*] Consumer A (Sukses) menunggu pesan...");

    channel.consume(queueName, (msg) => {
        if (msg) {
            console.log(` [v] Berhasil! Log & Update DB untuk data: ${msg.content.toString()}`);
            // Manual ACK
            channel.ack(msg);
        }
    }, { noAck: false });
}
runConsumerA();