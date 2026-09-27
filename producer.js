const amqp = require('amqplib');

async function runProducer() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'payments.event';

    // Mendeklarasikan Topic Exchange
    await channel.assertExchange(exchangeName, 'topic', { durable: true });

    // Simulasi kirim pembayaran sukses
    const msgSuccess = { id: 101, status: 'success' };
    channel.publish(exchangeName, 'payment.success', Buffer.from(JSON.stringify(msgSuccess)), { persistent: true });
    console.log(" [x] Mengirim payment.success");

    // Simulasi kirim pembayaran gagal
    const msgFailed = { id: 102, status: 'failed' };
    channel.publish(exchangeName, 'payment.failed', Buffer.from(JSON.stringify(msgFailed)), { persistent: true });
    console.log(" [x] Mengirim payment.failed");

    // Tutup koneksi setelah selesai
    setTimeout(() => { connection.close(); process.exit(0); }, 500);
}
runProducer();