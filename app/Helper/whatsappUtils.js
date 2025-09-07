const axios = require('axios');

const messageQueue = [];
const MAX_QUEUE_LENGTH = parseInt(process.env.WA_MAX_QUEUE_LENGTH || '10000', 10);
let isProcessingQueue = false;
let processMessageFunction;

const { logError } = require('./errorHandler');

class TokenBucket {
    constructor(capacity, fillPerSecond) {
        this.capacity = capacity;
        this.tokens = capacity;
        setInterval(() => this.addToken(), 1000 / fillPerSecond);
    }

    addToken() {
        if (this.tokens < this.capacity) {
            this.tokens++;
        }
    }

    take() {
        if (this.tokens > 0) {
            this.tokens--;
            return true;
        }
        return false;
    }
}

const rateLimiter = new TokenBucket(100, 10); // 10 requests per second

async function sendResponseToWhatsApp(chatId, message, whatsappPort, chatType) {
    try {
        let whatsapp_host = `http://localhost:${whatsappPort}`;

        if (chatType === 'private') {
            await axios.post(`${whatsapp_host}/send-message`, {
                number: chatId,
                message: message
            });
            return true;
        } else {
            await axios.post(`${whatsapp_host}/send-group-message`, {
                groupId: chatId,
                message: message
            });
            return true;
        }
    } catch (error) {
        throw error;
    }
}

function enqueueMessage(chatId, message, whatsappPort, chatType, messageDelay, messageType, secret_whatsapp) {
    if (messageQueue.length >= MAX_QUEUE_LENGTH) {
        // Drop oldest to keep memory bounded
        messageQueue.shift();
        // Best-effort log; avoid throwing if logger unavailable
        try { logError('WhatsApp message queue at capacity; dropping oldest'); } catch (_) {}
    }
    messageQueue.push({ chatId, message, whatsappPort, chatType, messageDelay, messageType, secret_whatsapp });
    if (!isProcessingQueue) {
        processMessageQueue();
    }
}

async function processMessageQueue() {
    if (messageQueue.length === 0) {
        isProcessingQueue = false;
        return;
    }

    isProcessingQueue = true;
    const { chatId, message, whatsappPort, chatType, messageDelay, messageType, secret_whatsapp } = messageQueue.shift();

    if (rateLimiter.take()) {
        try {
            if (messageType === 'checkTrue') {
                let result = await processMessageFunction(chatType, chatId, message, whatsappPort, secret_whatsapp, messageDelay);
            } else {
                await new Promise(resolve => setTimeout(resolve, messageDelay));

                await sendResponseToWhatsApp(chatId, message, whatsappPort, chatType);
            }
        } catch (error) {
            logError(`Error processing message for ${chatId}:`, error);
        }

        processMessageQueue();
    } else {
        setTimeout(() => {
            messageQueue.unshift({ chatId, message, whatsappPort, chatType, messageDelay, messageType, secret_whatsapp });
            processMessageQueue();
        }, 1000);
    }
}

function setProcessMessageFunction(func) {
    processMessageFunction = func;
}

module.exports = { sendResponseToWhatsApp, enqueueMessage, setProcessMessageFunction };
