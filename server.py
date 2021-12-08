import logging

from aiohttp import web


async def handle(request: web.Request):
    print('hey')
    return web.Response(text=f"Hey, Proxy here. {request.url}")


app = web.Application()
logging.basicConfig(level=logging.DEBUG)
app.add_routes([web.get("/{any:.*}", handle)])

if __name__ == "__main__":
    web.run_app(app, port=8080)
