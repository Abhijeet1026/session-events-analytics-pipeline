import json
import os
import random
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

GEO_CHOICES = ["Canada", "US"]
EVENT_TYPES = ["clicks", "signups", "log_in", "log_off", "delete_account", "pageview", "referral"]
PLATFORMS = ["web", "mobile"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]

# don’t mention any real brand; keep it generic
SITE_HOST = os.environ.get("SITE_HOST", "example-telecom.com")

def make_event(dt: str, platform: str):
    event_type = random.choice(EVENT_TYPES)

    base = {
        "id": uuid.uuid4().hex,  # universal id
        "session_id": f"sess-{uuid.uuid4().hex[:12]}",
        "attributes": {
            "username": fake.user_name() + "@" + SITE_HOST,
            "device": random.choice(["iPhone 15", "Pixel 8", "Samsung S23", "iPad", "MacBook Pro", "Windows Laptop"]),
            "os": random.choice(["iOS", "Android", "macOS", "Windows", "Linux"]),
            "geo": random.choice(GEO_CHOICES),
        },
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "platform": platform,
        "dt": dt,  # partition field for later parquet
    }

    if platform == "web":
        base.update({
            "url": f"https://{SITE_HOST}/" + random.choice([
                "home", "login", "account", "plans", "support", "checkout", "billing", "profile"
            ]),
            "browser_version": f"{random.choice(BROWSERS)}/{random.randint(90,130)}.0",
            "screen": None,
            "screen_size": None,
            "app_version": None,
        })
    else:
        base.update({
            "url": None,
            "browser_version": None,
            "screen": random.choice(["Home", "Login", "Account", "Plans", "Support", "Checkout"]),
            "screen_size": random.choice(["1080x2400", "1170x2532", "1440x3120"]),
            "app_version": f"{random.randint(1,5)}.{random.randint(0,20)}.{random.randint(0,50)}",
        })

    return base

def lambda_handler(event, context):
    try:
        body = event.get("body") or "{}"
        if isinstance(body, str):
            body = json.loads(body)

        count = int(body.get("count", 1000))
        # Keep it safe for API responses
        if count < 1 or count > 5000:
            return {"statusCode": 400, "body": json.dumps({"error": "count must be 1..5000"})}

        dt = body.get("dt") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        platform = body.get("platform") or random.choice(PLATFORMS)
        if platform not in PLATFORMS:
            return {"statusCode": 400, "body": json.dumps({"error": "platform must be web or mobile"})}

        events = [make_event(dt, platform) for _ in range(count)]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "dt": dt,
                "count": count,
                "platform": platform,
                "batch_id": uuid.uuid4().hex,
                "events": events
            })
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
