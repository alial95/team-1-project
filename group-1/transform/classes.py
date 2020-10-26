class Transaction:

    def __init__(self, date, time, location, customer, total):
        self.date = date
        self.time = time
        self.location = location
        self.customer = customer
        self.total = total

class Basket:
    def __init__(self, basket_item, price):
        self.basket_item = basket_item
        self.price = price
        