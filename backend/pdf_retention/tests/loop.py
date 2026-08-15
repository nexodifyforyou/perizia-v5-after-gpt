import asyncio

TEST_LOOP = asyncio.new_event_loop()


def run(coro):
    return TEST_LOOP.run_until_complete(coro)
