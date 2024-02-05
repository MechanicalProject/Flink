from bookTicker import produce_bookTicker
from price import produce_price
from multiprocessing import Process

def run_produce_bookTicker():
    produce_bookTicker.start()

# produce_price를 실행하는 함수
def run_produce_price():
    produce_price.start()
    
if __name__ == "__main__":
    process_bookTicker = Process(target=run_produce_bookTicker)
    process_price = Process(target=run_produce_price)

    process_bookTicker.start()
    process_price.start()

    process_bookTicker.join()
    process_price.join()