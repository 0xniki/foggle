import uvloop
import logging

from src.core import Foggle

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    await Foggle().run()

if __name__ == "__main__":
    uvloop.run(main())
