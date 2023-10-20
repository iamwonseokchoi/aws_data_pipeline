import time
import random
import concurrent
from concurrent.futures import ThreadPoolExecutor
from functions import handle_data, logger


if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        while True:
            # Submit 5 tasks concurrently
            futures = [executor.submit(handle_data, i) for i in range(5)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Exception occurred in future: {e}")
            time.sleep(random.randint(10, 20))    # Random sleep to just add some variation in the stream
