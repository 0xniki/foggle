import uvloop
import logging

from src.core import Foggle

async def main():
    logging.basicConfig(level=logging.INFO)

    await Foggle().run()

if __name__ == "__main__":
    uvloop.run(main())
