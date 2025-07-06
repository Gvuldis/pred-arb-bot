# notifications/discord.py

import requests
webhook_url="https://discord.com/api/webhooks/1255893289136160869/ZwX3Qo1JsF_fBD0kdmI8-xaEyvah9TnAV_R7dIHIKdBAwpEvj6VgmP3YcOa7j8zpyAPN"
class DiscordNotifier:
    def __init__(self, webhook_url:str):
        self.url = webhook_url

    def alert(self, opportunity:dict):
        text = (f"🎯 ARB: {opportunity['bodega_name']}\n"
                f"{opportunity['side']}  YES@${opportunity['price_yes_usd']:.2f} / NO@${opportunity['price_no_usd']:.2f}\n"
                f"Profit ${opportunity['profit']:.2f} ({opportunity['roi']*100:.1f}%)")
        if self.url:
            requests.post(self.url, json={"content":text})
        else:
            print(text)
