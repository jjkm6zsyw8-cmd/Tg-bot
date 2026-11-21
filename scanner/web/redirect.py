"""HTTPS-страница перехода в приложение Pump.fun (разд. 8, 12 ТЗ).

URL-кнопки Telegram принимают только http(s) и tg://, поэтому кнопка ведёт на
https://<домен>/p/<адрес>. На телефоне страница открывает pump://coin/<адрес>,
а если приложение не открылось за 1.5 с — https://pump.fun/coin/<адрес>.
На компьютере — сразу веб-версия. Принимается только валидный адрес токена,
переходы только на pump:// и pump.fun — открытого редиректа нет.
"""
from __future__ import annotations

from aiohttp import web

from ..solana import is_valid_address

PAGE = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>Открываю Pump.fun…</title>
<style>body{{font-family:system-ui,sans-serif;background:#0b0f14;color:#e6edf3;display:flex;
min-height:100vh;align-items:center;justify-content:center;margin:0}}
main{{text-align:center;padding:24px}}a{{color:#7ee787}}</style></head>
<body><main><p>Открываю монету в приложении Pump.fun…</p>
<p><a href="{app}">Открыть в приложении</a> · <a href="{web}">Открыть на сайте</a></p></main>
<script>
(function(){{
  var app = {app_js}, web = {web_js};
  var mobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
  if (!mobile) {{ window.location.replace(web); return; }}
  var t = setTimeout(function(){{ window.location.replace(web); }}, 1500);
  document.addEventListener('visibilitychange', function(){{ if (document.hidden) clearTimeout(t); }});
  window.location.href = app;
}})();
</script></body></html>"""


async def pump_redirect(request: web.Request) -> web.Response:
    mint = request.match_info.get("mint", "")
    if not is_valid_address(mint):
        return web.Response(status=400, text="bad address")
    app_url = f"pump://coin/{mint}"
    web_url = f"https://pump.fun/coin/{mint}"
    body = PAGE.format(app=app_url, web=web_url, app_js=f'"{app_url}"', web_js=f'"{web_url}"')
    return web.Response(text=body, content_type="text/html", headers={
        "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
        "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'",
    })


async def health(_request: web.Request) -> web.Response:
    return web.Response(text="ok")


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/p/{mint}", pump_redirect)
    app.router.add_get("/healthz", health)
    return app


async def run_web(host: str, port: int) -> None:
    runner = web.AppRunner(make_app())
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    import asyncio
    await asyncio.Event().wait()
