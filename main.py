from fastapi import FastAPI
import asyncio

app = FastAPI()


@app.get("/")
def root():
    return "Hello World!!"


@app.get("/work/{seconds}")
async def do_work(seconds: int):
    await asyncio.sleep(seconds)  # simulate I/O wait
    return {"done_in": seconds}
