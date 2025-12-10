from asyncio.log import logger
import json
import os
from pywebpush import webpush


def send_push_message(subscription: dict, title: str, description: str):
    try:
        webpush(
            subscription_info=subscription,
            data=json.dumps({"title": title, "description": description}),
            vapid_private_key=os.environ.get("F1STATS_VAPID_PRIVATE"),
            vapid_claims={"sub": "mailto:divinasctr@gmail.com"},
        )
    except:
        logger.error("Unable to send message to subscription: ", subscription)
