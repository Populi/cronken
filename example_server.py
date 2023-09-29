import asyncio
import signal

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


async def main():
    loop = asyncio.get_running_loop()
    cronken = Cronken(redis_info={"host": "localhost", "port": 6379})

    async def graceful_shutdown(sig: signal.Signals):
        cronken.logger.error(f"Received signal {sig}, gracefully shutting down...")
        await cronken.cleanup()
        cronken.logger.error("Finished cleanup")

    # Jobs persist in redis and are automatically loaded on cronken startup
    cronken.logger.info("Starting cronken...")
    cronken_lifetime = await cronken.start()
    # Schedule cleanup on SIGINT
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(graceful_shutdown(sig)))
    # For the purposes of this example, we'll overwrite the jobs in redis with the example above
    cronken.logger.info("Setting jobs...")
    await cronken.set_jobs(test_jobs)
    # Now that we've overwritten the jobs, reload them
    cronken.logger.info("Reloading jobs...")
    await cronken.reload_jobs()
    cronken.logger.info("Running...")
    await cronken_lifetime


if __name__ == '__main__':
    asyncio.run(main())
