USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--window-size=1280,800",
    "--window-position=-32000,-32000",
    f"--user-agent={USER_AGENT}",
]

# Rate limiting
DELAY_BETWEEN_ARTICLES = (3.0, 7.0)  # seconds (random in range)
DELAY_AFTER_NAV = (0.5, 1.5)

# Retries
RETRY_MAX = 3
RETRY_BACKOFF_BASE = 2.0  # 2s, 4s, 8s ...

# Timeouts (seconds)
NAV_WAIT = 30
WAIT_LONG = 15
WAIT_SHORT = 3
ARTICLE_TIMEOUT = 300  # 5 min max per article before treating as stuck
MAX_TIMEOUT_RETRIES = 3  # attempts before giving up on that article
BLOCK_RECOVERY_TIMEOUT = 300
