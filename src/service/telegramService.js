const sendTelegramMessage = async (message, chatId = process.env.TELEGRAM_UPWORK_JOB_SCRAPER_CHAT_ID) => {

    const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

    const TELEGRAM_API_URL = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;

    const data = {
        chat_id: chatId,
        text: message,
        parse_mode: "Markdown",
    };

    try {
        await fetch(TELEGRAM_API_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(data),
        });

    } catch (error) {
        console.error("Error:", error);
    }
};

export { sendTelegramMessage };