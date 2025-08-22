import { ProxyAgent, fetch as undisciFetch } from 'undici';
import {sendTelegramMessage} from "../service/telegramService.js";

export async function checkProxyConnection(proxyAgent, retries = 3) {
    const delays = [5000, 10000, 15000]; // ms delays between retries

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            console.log(`Attempt ${attempt}: Checking proxy connection...`);
            const res = await undisciFetch('https://api.ipify.org?format=json', {
                dispatcher: proxyAgent,
                signal: AbortSignal.timeout(15000)
            });

            if (!res.ok) throw new Error(`HTTP ${res.status}`);

            const data = await res.json();
            console.log(`✅ Proxy works. IP: ${data.ip}`);
            //sendTelegramMessage(`✅ Proxy works. IP: ${data.ip}`)
            return true;
        } catch (err) {
            console.error(`❌ Proxy check failed: ${err.message}`);
            sendTelegramMessage(`❌ Proxy check failed: ${err.message}`)
            if (attempt < retries) {
                const wait = delays[attempt - 1];
                console.log(`⏳ Retrying in ${wait / 1000} seconds...`);
                sendTelegramMessage(`⏳ Retrying in ${wait / 1000} seconds...`)
                await delay(wait);
            }
        }
    }
    console.error('❌ Proxy connection failed after all retries.');
    sendTelegramMessage('❌ Proxy connection failed after all retries.')
    return false;
}