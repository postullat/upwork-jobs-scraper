import 'dotenv/config'
import puppeteer from 'puppeteer-extra'
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
import {sendTelegramMessage} from "./service/telegramService.js";

puppeteer.use(StealthPlugin())

const USE_PROXY = true
const proxyHost = process.env.PROXY_HOST;
const proxyPort = process.env.PROXY_PORT;
const proxyUser = process.env.PROXY_USER;
const proxyPass = process.env.PROXY_PASS;
const masterRefresh7Token = process.env.MASTER_REFRESH_TOKEN;

export async function getOAuth2v2Cookies() {
    let browser
    let page
    try {
        const launchOptions = {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-web-security',
                ...(USE_PROXY ? [`--proxy-server=http://${proxyHost}:${proxyPort}`] : []),
            ],
        }

        browser = await puppeteer.launch(launchOptions)
        page = await browser.newPage()

        // Set up proxy authentication if using proxy
        if (USE_PROXY) {
            await page.authenticate({
                username: proxyUser,
                password: proxyPass,
            })
        }

        // Set extra HTTP headers
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        // Pre-seed Upwork master token cookie
        try {
            if (masterRefresh7Token) {
                const expires = Math.floor(Date.now() / 1000) + 10 * 24 * 3600
                await page.setCookie({
                    name: 'master_refresh_token',
                    value: String(masterRefresh7Token),
                    domain: '.upwork.com',
                    path: '/',
                    httpOnly: false,
                    secure: true,
                    sameSite: 'None',
                    expires: expires,
                })
                console.log('🔑 master_access_token cookie set for .upwork.com')
            } else {
                console.log('ℹ️ UPWORK_MASTER_TOKEN is not set; skipping master_access_token cookie')
            }
        } catch (e) {
            console.log('⚠️ Failed to set master_access_token cookie:', e?.message)
        }

        await page.goto('https://www.upwork.com/ab/account-security/login')
        await new Promise(resolve => setTimeout(resolve, 3000))

        // Get all cookies and filter those with oauth2v2 values
        try {
            const allCookies = await page.cookies()
            const oauth2v2Cookies = Array.isArray(allCookies)
                ? allCookies.filter(c => c && c.value && c.value.startsWith('oauth2v2'))
                : []

            console.log('🍪 OAuth2v2 cookies count:', oauth2v2Cookies.length)
            let selectedCookies = [];

            for (let c of oauth2v2Cookies) {
                try {
                    const expiresAt = c.expires
                        ? new Date(c.expires * 1000).toString() // convert seconds → ms
                        : "Session (no expiration)";

                    console.log("🍪 OAuth2v2 cookie:", {
                        name: c.name,
                        value: c.value,
                        domain: c.domain,
                        path: c.path,
                        secure: c.secure,
                        sameSite: c.sameSite,
                        expires: c.expires,
                        expiresReadable: expiresAt
                    });

                    selectedCookies.push(
                        `🍪 OAuth2v2 cookie:
                              name: ${c.name},
                              value: ${escapeTelegramMarkdown(c.value)},
                              domain: ${c.domain},
                              path: ${c.path},
                              secure: ${c.secure},
                              sameSite: ${c.sameSite},
                              expires: ${c.expires},
                              expiresReadable: ${expiresAt}`
                    );
                } catch {}
            }

            // send all at once after loop
            if (selectedCookies.length > 0) {
                await sendTelegramMessage(selectedCookies.join("\n\n"));
            }


            return oauth2v2Cookies
        } catch (e) {
            console.log('🍪 Failed to read cookies:', e?.message)
            return []
        }
    } catch (err) {
        console.error('❌ Puppeteer flow failed:', err?.message)
        throw err
    } finally {
        if (page) await page.close()
        if (browser) await browser.close()
    }
}

function escapeTelegramMarkdown(text) {
    return text.replace(/([_*[\]()~`>#+\-=|{}.!])/g, '\\$1');
}



/*async function puppeteerCookies() {
    try {
        console.log('🔍 Starting cookie fetch...')
        const cookies = await getOAuth2v2Cookies()
        //console.log('✅ Found cookies:', cookies)
    } catch (error) {
        console.error('❌ Error:', error)
    }
}

puppeteerCookies()*/
