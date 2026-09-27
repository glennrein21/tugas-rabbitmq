const amqp = require('amqplib');

async function runConsumerB() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeMain = 'payments.event';
    const exchangeDLX = 'payments.dlx';

    await channel.assertExchange(exchangeMain, 'topic', { durable: true });
    await channel.assertExchange(exchangeDLX, 'direct', { durable: true });

    // 1. Setup Dead Letter Queue (DLQ)
    await channel.assertQueue('q_payment_dlq', { durable: true });
    await channel.bindQueue('q_payment_dlq', exchangeDLX, 'payment.failed');

    // 2. Setup Queue Tunggu/Delay (TTL 5 detik)
    await channel.assertQueue('q_payment_wait', {
        durable: true,
        arguments: {
            'x-message-ttl': 5000, 
            'x-dead-letter-exchange': exchangeMain,
            'x-dead-letter-routing-key': 'payment.failed'
        }
    });

    // 3. Setup Queue Gagal Utama
    await channel.assertQueue('q_payment_failed', {
        durable: true,
        arguments: {
            'x-dead-letter-exchange': exchangeDLX,
            'x-dead-letter-routing-key': 'payment.failed'
        }
    });
    await channel.bindQueue('q_payment_failed', exchangeMain, 'payment.failed');

    channel.prefetch(1);
    console.log(" [*] Consumer B (Gagal) menunggu pesan...");

    channel.consume('q_payment_failed', (msg) => {
        if (msg) {
            // Cek sudah berapa kali di-retry
            let retryCount = msg.properties.headers && msg.properties.headers['x-retry-count'] ? msg.properties.headers['x-retry-count'] : 0;
            
            console.log(` [X] Menerima data error. Percobaan ke-${retryCount}`);

            if (retryCount < 2) {
                console.log(`     -> Mencoba ulang (Retry). Menunggu 5 detik...`);
                // Lempar ke antrean tunggu (q_payment_wait)
                channel.publish('', 'q_payment_wait', msg.content, {
                    headers: { 'x-retry-count': retryCount + 1 },
                    persistent: true
                });
                channel.ack(msg);
            } else {
                console.log(`     -> Maksimal retry tercapai! Melempar ke DLX.`);
                // Tolak pesan, akan otomatis masuk ke DLQ
                channel.nack(msg, false, false);
            }
        }
    }, { noAck: false });
}
runConsumerB();