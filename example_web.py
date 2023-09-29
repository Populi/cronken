from aiohttp import web
from cronken import Cronken

test_jobs = {
    "minutely": {
        "cron_args": {
            "cronstring": "* * * * *"
        },
        "job_args": {
            "cmd": "echo 'minutely job'",
            "lock": True,
            "ttl": 10
        }
    }
}

routes = web.RouteTableDef()


@routes.get("/healthcheck")
async def healthcheck(request):
    cronken: Cronken = request.app["cronken"]

    # If we've failed our status check, log it and return 500
    if not all(x == 0 for x in cronken.state.values()):
        return web.json_response(cronken.state, status=500)

    # Otherwise, return the response with 200 OK
    return web.json_response(cronken.state, status=200)


@routes.post("/send_event")
async def send_event(request):
    # This is a very simple example of how you'd send command-and-control events via the web interface.
    # In a real-world system, you wouldn't want this exposed to the world, as it would allow anyone who
    # had access to the endpoint to alter jobs at will.
    cronken: Cronken = request.app["cronken"]
    event_text = await request.text()
    cronken.logger.debug(f"Received request to send event {event_text}")
    try:
        await cronken.send_event(event_text)
    except Exception as e:
        cronken.logger.debug(f"Failed to send event: {e!r}")
        return web.json_response({"message": f"Failed to send event: {e!r}"}, status=400)
    return web.json_response({"message": "Sent event successfully"}, status=200)


@routes.post("/update_jobs")
async def update_jobs(request):
    cronken: Cronken = request.app["cronken"]
    try:
        jobs_dict = await request.json()
        await cronken.set_jobs(jobs_dict)
    except Exception as e:
        cronken.logger.debug(f"Failed to update jobs: {e!r}")
        return web.json_response({"message": f"Failed to update jobs: {e!r}"}, status=500)
    return web.json_response({"message": "Updated jobs successfully"}, status=200)


async def cronken_start(app: web.Application):
    cronken: Cronken = app["cronken"]
    await cronken.start()
    # For the purposes of this example, we'll overwrite the jobs in redis with the example above
    cronken.logger.info("Setting jobs...")
    await cronken.set_jobs(test_jobs)
    # Now that we've overwritten the jobs, reload them
    cronken.logger.info("Reloading jobs...")
    await cronken.reload_jobs()


async def cronken_cleanup(app: web.Application):
    await app["cronken"].cleanup()


def main():
    app = web.Application()
    app.add_routes(routes)
    cronken = Cronken(redis_info={"host": "localhost", "port": 6379})
    app["cronken"] = cronken
    app.on_startup.append(cronken_start)
    app.on_cleanup.append(cronken_cleanup)
    # Once this is running and the first job triggers, you should be able to curl
    # http://localhost:8085/healthcheck to get the last run's return code.
    web.run_app(app, host="localhost", port=8085)


if __name__ == '__main__':
    main()
